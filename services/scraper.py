"""
Server member scraper for Discord Mass DM Tool.

This module provides functionality for scraping members from Discord servers.
"""

import logging
import asyncio
import time
import aiohttp
from typing import Dict, List, Optional, Any, Tuple, Set

# 注意这里使用discord.py-self
import discord
from discord.ext import commands

from core.token_manager import TokenManager
from core.user_manager import UserManager
from utils.helpers import is_snowflake

logger = logging.getLogger("discord_dm_tool")


class MemberScraper:
    """
    Scrapes members from Discord servers.
    
    Uses Discord tokens to fetch members from servers and saves them to the UserManager.
    """
    
    def __init__(self, config: Dict[str, Any], token_manager: TokenManager, user_manager: UserManager):
        """
        Initialize the MemberScraper.
        
        Args:
            config (Dict[str, Any]): The application configuration.
            token_manager (TokenManager): Token manager instance.
            user_manager (UserManager): User manager instance.
        """
        self.config = config
        self.token_manager = token_manager
        self.user_manager = user_manager
        
        self.scrape_settings = self.config.get("scraping", {})
        self.max_members = self.scrape_settings.get("max_members_per_server", 1000)
        self.scrape_timeout = self.scrape_settings.get("scrape_timeout", 60)
        
        # Store last scrape time per server to prevent too frequent scraping
        self.last_scrape = {}
        
        logger.debug("MemberScraper initialized")
    
    async def scrape_server(self, server_id: str, token: Optional[str] = None) -> Tuple[int, int, str]:
        """
        Scrape members from a Discord server.
        
        Args:
            server_id (str): The ID of the server to scrape.
            token (Optional[str], optional): The Discord token to use. If None, a token will be
                selected from the token manager. Defaults to None.
                
        Returns:
            Tuple[int, int, str]: A tuple containing:
                - Number of members scraped
                - Number of new members added to the user manager
                - Status message
        """
        # Validate server ID
        if not is_snowflake(server_id):
            logger.error(f"Invalid server ID: {server_id}")
            return 0, 0, "Invalid server ID format"
        
        # Get token if not provided
        if token is None:
            token = self.token_manager.get_next_token()
            if token is None:
                logger.error("No tokens available for scraping")
                return 0, 0, "No tokens available"
        
        # 记录使用的令牌信息
        masked_token = token[:10] + "..." + token[-5:] if len(token) > 15 else "***"
        logger.info(f"Using token: {masked_token} for server {server_id}")
        
        # Check if we've scraped this server recently (within last 15 minutes)
        current_time = time.time()
        if server_id in self.last_scrape and current_time - self.last_scrape[server_id] < 900:
            time_since = int(current_time - self.last_scrape[server_id])
            logger.warning(f"Server {server_id} was scraped {time_since} seconds ago. Waiting...")
            return 0, 0, f"Server was scraped recently, please wait {900 - time_since} seconds"
        
        # 首先通过API直接检查令牌和服务器权限
        try:
            logger.info("Testing token via direct API...")
            headers = {
                'Authorization': token,
                'Content-Type': 'application/json'
            }
            
            async with aiohttp.ClientSession() as session:
                # 测试用户信息
                async with session.get('https://discord.com/api/v9/users/@me', headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Token validation failed: Status {response.status} - {error_text}")
                        return 0, 0, f"Invalid token: Status {response.status}"
                    
                    user_data = await response.json()
                    logger.info(f"Authenticated as: {user_data.get('username')} (ID: {user_data.get('id')})")
                
                # 测试服务器权限
                async with session.get(f'https://discord.com/api/v9/guilds/{server_id}', headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Server access failed: Status {response.status} - {error_text}")
                        if response.status == 403:
                            return 0, 0, "No permission to access this server"
                        elif response.status == 404:
                            return 0, 0, "Server not found"
                        return 0, 0, f"Server access error: Status {response.status}"
                    
                    guild_data = await response.json()
                    logger.info(f"Access to server confirmed: {guild_data.get('name')}")
                
                # 尝试直接通过API抓取成员
                logger.info("Attempting to scrape members directly via API...")
                scraped_members = []
                new_members = 0
                after = "0"  # 起始ID
                
                while len(scraped_members) < self.max_members:
                    endpoint = f'https://discord.com/api/v9/guilds/{server_id}/members?limit=1000'
                    if after != "0":
                        endpoint += f'&after={after}'
                    
                    async with session.get(endpoint, headers=headers) as response:
                        if response.status != 200:
                            logger.warning(f"Direct member fetch failed: Status {response.status}")
                            break
                        
                        members_data = await response.json()
                        if not members_data or len(members_data) == 0:
                            logger.info("No more members to fetch")
                            break
                        
                        # 处理成员数据
                        logger.info(f"Fetched {len(members_data)} members via API")
                        
                        for member_data in members_data:
                            user_data = member_data.get('user', {})
                            
                            # 跳过机器人
                            if user_data.get('bot', False):
                                continue
                                
                            user_id = str(user_data.get('id'))
                            username = user_data.get('username', 'Unknown')
                            
                            # 更新最后处理的ID
                            after = user_id
                            
                            # 添加到列表
                            scraped_members.append({
                                "user_id": user_id,
                                "username": username,
                                "discriminator": user_data.get('discriminator', ''),
                                "avatar": user_data.get('avatar', ''),
                                "joined_at": member_data.get('joined_at')
                            })
                            
                            # 添加到用户管理器
                            if not self.user_manager.get_user_by_discord_id(user_id):
                                metadata = {
                                    "username": username,
                                    "discriminator": user_data.get('discriminator', ''),
                                    "avatar": user_data.get('avatar', ''),
                                    "source_server": server_id,
                                    "server_name": guild_data.get('name', 'Unknown'),
                                    "joined_at": member_data.get('joined_at'),
                                    "scraped_at": time.time()
                                }
                                
                                success, _ = self.user_manager.add_user(user_id, username, metadata)
                                if success:
                                    new_members += 1
                            
                            if len(scraped_members) >= self.max_members:
                                logger.warning(f"Reached maximum member limit of {self.max_members}")
                                break
                    
                    # 为避免达到API速率限制，添加短暂延迟
                    await asyncio.sleep(1)
                
                # 如果API抓取成功，直接返回结果
                if len(scraped_members) > 0:
                    logger.info(f"Successfully scraped {len(scraped_members)} members via direct API")
                    
                    # 更新最后抓取时间
                    self.last_scrape[server_id] = current_time
                    
                    # 添加服务器到配置（如果还不存在）
                    servers = self.scrape_settings.get("servers", [])
                    if server_id not in servers:
                        servers.append(server_id)
                        self.scrape_settings["servers"] = servers
                        self.config["scraping"] = self.scrape_settings
                    
                    return len(scraped_members), new_members, "Success"
                
                logger.warning("Direct API scraping returned no members, falling back to client method")
        
        except Exception as e:
            logger.error(f"Error during API testing or direct scraping: {e}")
            # 继续尝试使用客户端方法
        
        # 回退到使用discord.py-self客户端方法
        logger.info("Setting up Discord client...")
        
        try:
            # 设置intents
            intents = discord.Intents.all()
        except:
            logger.warning("Could not set all intents, using default")
            intents = discord.Intents.default()
            intents.members = True
            intents.guilds = True
        
        client = commands.Bot(command_prefix="!", intents=intents, self_bot=True)
        
        # Track scraped members
        scraped_members = []
        new_members = 0
        
        @client.event
        async def on_ready():
            """Called when the client is ready."""
            logger.info(f"Logged in as {client.user.name} ({client.user.id})")
            
            try:
                # Get the guild
                guild = client.get_guild(int(server_id))
                if guild is None:
                    # Try to fetch the guild
                    try:
                        guild = await client.fetch_guild(int(server_id))
                    except discord.errors.Forbidden:
                        logger.error(f"No access to server {server_id}")
                        await client.close()
                        return
                    except Exception as e:
                        logger.error(f"Failed to fetch server {server_id}: {e}")
                        await client.close()
                        return
                
                logger.info(f"Scraping members from server: {guild.name} ({guild.id})")
                logger.info(f"Server has approximately {guild.member_count} members")
                
                # 尝试多种方法获取成员
                try:
                    # 方法1: 使用成员缓存
                    if hasattr(guild, 'members') and len(guild.members) > 0:
                        logger.info(f"Using cached members: {len(guild.members)} available")
                        on_member_chunk(guild, guild.members)
                    
                    # 方法2: 请求成员分块
                    logger.info("Requesting member chunks...")
                    await guild.chunk()
                    
                    # 等待成员接收
                    logger.info(f"Waiting {self.scrape_timeout} seconds for member chunks...")
                    await asyncio.sleep(self.scrape_timeout)
                except Exception as e:
                    logger.error(f"Error during member scraping: {e}")
                
                await client.close()
            
            except Exception as e:
                logger.error(f"Error during server scraping: {e}")
                await client.close()
        
        # 定义on_member_chunk事件处理程序作为普通函数
        def on_member_chunk(guild, members):
            """Process a chunk of members."""
            nonlocal new_members
            
            logger.info(f"Processing chunk with {len(members)} members")
            
            # Add each member to the list
            for member in members:
                if len(scraped_members) >= self.max_members:
                    logger.warning(f"Reached maximum member limit of {self.max_members}")
                    break
                
                # Skip bots
                if hasattr(member, 'bot') and member.bot:
                    continue
                
                user_id = str(member.id)
                
                # Skip if already in list
                if any(m.get("user_id") == user_id for m in scraped_members):
                    continue
                
                username = member.name if hasattr(member, 'name') else "Unknown"
                
                # Add to scraped members list
                scraped_members.append({
                    "user_id": user_id,
                    "username": username,
                    "discriminator": member.discriminator if hasattr(member, 'discriminator') else "",
                    "avatar": str(member.avatar.url) if hasattr(member, 'avatar') and member.avatar else "",
                    "joined_at": member.joined_at.timestamp() if hasattr(member, 'joined_at') and member.joined_at else None
                })
                
                # Add to user manager if it doesn't exist
                if not self.user_manager.get_user_by_discord_id(user_id):
                    metadata = {
                        "username": username,
                        "discriminator": member.discriminator if hasattr(member, 'discriminator') else "",
                        "avatar": str(member.avatar.url) if hasattr(member, 'avatar') and member.avatar else "",
                        "source_server": str(guild.id),
                        "server_name": guild.name if hasattr(guild, 'name') else "Unknown",
                        "joined_at": member.joined_at.timestamp() if hasattr(member, 'joined_at') and member.joined_at else None,
                        "scraped_at": time.time()
                    }
                    
                    success, _ = self.user_manager.add_user(user_id, username, metadata)
                    if success:
                        new_members += 1
        
        # 注册事件处理程序
        client.event(on_member_chunk)
        
        # Run the client
        try:
            # Update last scrape time
            self.last_scrape[server_id] = current_time
            
            # Start the client
            logger.info("Starting Discord client...")
            await client.start(token)
        except discord.errors.LoginFailure as e:
            logger.error(f"Invalid Discord token: {e}")
            return 0, 0, f"Invalid Discord token: {e}"
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return 0, 0, f"Error: {e}"
        
        logger.info(f"Scraped {len(scraped_members)} members, added {new_members} new members")
        
        # Add server to the configuration if it's not already there
        servers = self.scrape_settings.get("servers", [])
        if server_id not in servers:
            servers.append(server_id)
            self.scrape_settings["servers"] = servers
            self.config["scraping"] = self.scrape_settings
        
        if len(scraped_members) > 0:
            return len(scraped_members), new_members, "Success"
        else:
            return 0, 0, "No members were scraped (possibly no permission to view members)"
    
    def get_scrape_history(self) -> Dict[str, Dict[str, Any]]:
        """
        Get scraping history.
        
        Returns:
            Dict[str, Dict[str, Any]]: Dictionary mapping server IDs to their last scrape time.
        """
        history = {}
        
        for server_id, timestamp in self.last_scrape.items():
            history[server_id] = {
                "server_id": server_id,
                "last_scrape": timestamp,
                "formatted_time": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))
            }
        
        return history
    
    def clear_scrape_history(self) -> None:
        """Clear scraping history."""
        self.last_scrape = {}
        logger.info("Scrape history cleared")