"""
Server member scraper for Discord Mass DM Tool.

This module provides functionality for scraping members from Discord servers.
"""

import logging
import asyncio
import time
import aiohttp
from typing import Dict, List, Optional, Any, Tuple, Set

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
        if token:
            masked_token = token[:10] + "..." + token[-5:] if len(token) > 15 else "***"
            logger.info(f"Using token: {masked_token} for server {server_id}")
        
        # Check if we've scraped this server recently (within last 15 minutes)
        current_time = time.time()
        if server_id in self.last_scrape and current_time - self.last_scrape[server_id] < 900:
            time_since = int(current_time - self.last_scrape[server_id])
            logger.warning(f"Server {server_id} was scraped {time_since} seconds ago. Waiting...")
            return 0, 0, f"Server was scraped recently, please wait {900 - time_since} seconds"
        
        # 首先测试令牌有效性和基本权限
        try:
            logger.info("Testing token with Discord API...")
            headers = {
                'Authorization': token,
                'Content-Type': 'application/json'
            }
            
            async with aiohttp.ClientSession() as session:
                # 测试用户信息端点
                async with session.get('https://discord.com/api/v10/users/@me', headers=headers) as response:
                    status = response.status
                    logger.info(f"API test response code: {status}")
                    
                    if status == 200:
                        user_data = await response.json()
                        logger.info(f"Successfully authenticated as user: {user_data.get('username', 'unknown')}")
                    else:
                        error_text = await response.text()
                        logger.error(f"API test failed: Status {status} - {error_text}")
                        return 0, 0, f"Invalid token or API error: {status}"
                
                # 尝试获取服务器信息
                logger.info(f"Testing server access to ID: {server_id}")
                async with session.get(f'https://discord.com/api/v10/guilds/{server_id}', headers=headers) as response:
                    status = response.status
                    logger.info(f"Server access test response code: {status}")
                    
                    if status == 200:
                        guild_data = await response.json()
                        logger.info(f"Successfully accessed server: {guild_data.get('name', 'unknown')}")
                    else:
                        error_text = await response.text()
                        logger.error(f"Server access test failed: Status {status} - {error_text}")
                        if status == 403:
                            return 0, 0, "No access to server (Forbidden)"
                        elif status == 404:
                            return 0, 0, "Server not found"
        except Exception as e:
            logger.error(f"API test exception: {str(e)}")
            return 0, 0, f"API connection error: {str(e)}"
        
        # 设置Discord客户端，尝试使用所有可能的intents
        logger.info("Setting up Discord client with enhanced intents...")
        try:
            intents = discord.Intents.all()  # 尝试使用所有可用的intents
        except Exception as e:
            logger.warning(f"Could not create all intents, falling back to default+members: {e}")
            intents = discord.Intents.default()
            intents.members = True
            intents.guilds = True
        
        client = commands.Bot(command_prefix="!", intents=intents)
        
        # Track scraped members
        scraped_members = []
        new_members = 0
        scrape_success = False
        error_message = "Operation timed out or failed"
        
        @client.event
        async def on_ready():
            """Called when the client is ready."""
            nonlocal scrape_success, error_message
            
            logger.info(f"Logged in as {client.user.name} ({client.user.id})")
            
            try:
                # 获取服务器信息
                logger.info(f"Attempting to get guild {server_id}")
                guild = client.get_guild(int(server_id))
                
                if guild is None:
                    logger.warning(f"Could not get guild {server_id} using get_guild, trying fetch_guild...")
                    try:
                        guild = await client.fetch_guild(int(server_id))
                        logger.info(f"Successfully fetched guild: {guild.name}")
                    except discord.errors.Forbidden as e:
                        logger.error(f"No access to server {server_id}: {e}")
                        error_message = f"No access to server: {str(e)}"
                        await client.close()
                        return
                    except Exception as e:
                        logger.error(f"Failed to fetch server {server_id}: {e}")
                        error_message = f"Failed to fetch server: {str(e)}"
                        await client.close()
                        return
                
                logger.info(f"Scraping members from server: {guild.name} ({guild.id})")
                
                # 尝试多种方式获取成员
                try:
                    # 方法1: 使用chunk
                    logger.info("Attempting to fetch members using chunk() method...")
                    await guild.chunk()
                    
                    # 方法2: 如果chunk没有触发事件，尝试直接获取成员
                    if len(scraped_members) == 0:
                        logger.info("No members received from chunk(), trying alternative methods...")
                        try:
                            # 直接从guild.members获取
                            logger.info("Attempting to access guild.members directly...")
                            members = guild.members
                            logger.info(f"Direct access returned {len(members)} members")
                            
                            # 处理成员
                            await process_members(guild, members)
                        except Exception as e:
                            logger.error(f"Direct member access failed: {e}")
                    
                    # 等待一段时间以接收member chunks
                    logger.info(f"Waiting {self.scrape_timeout} seconds for member chunks...")
                    await asyncio.sleep(self.scrape_timeout)
                    
                    scrape_success = True
                except Exception as e:
                    logger.error(f"Error scraping members: {e}")
                    error_message = f"Error scraping members: {str(e)}"
                
                # 关闭客户端
                await client.close()
            
            except Exception as e:
                logger.error(f"Error during server scraping: {e}")
                error_message = f"Error during scraping: {str(e)}"
                await client.close()
        
        @client.event
        async def on_member_chunk(guild, members):
            """Called when a chunk of members is received."""
            logger.info(f"Received member chunk with {len(members)} members")
            await process_members(guild, members)
        
        async def process_members(guild, members):
            """Process a list of members and add them to the scraped list."""
            nonlocal new_members
            
            # 添加每个成员到列表
            member_count = 0
            for member in members:
                if len(scraped_members) >= self.max_members:
                    logger.warning(f"Reached maximum member limit of {self.max_members}")
                    break
                
                # 跳过机器人
                if member.bot:
                    continue
                
                user_id = str(member.id)
                username = f"{member.name}"
                
                # 检查是否已经添加过这个用户ID
                if any(m.get("user_id") == user_id for m in scraped_members):
                    continue
                
                # 添加到scraped_members列表
                scraped_members.append({
                    "user_id": user_id,
                    "username": username,
                    "discriminator": member.discriminator if hasattr(member, 'discriminator') else "",
                    "avatar": str(member.avatar.url) if member.avatar else "",
                    "joined_at": member.joined_at.timestamp() if member.joined_at else None
                })
                member_count += 1
                
                # 添加到user manager（如果不存在）
                if not self.user_manager.get_user_by_discord_id(user_id):
                    metadata = {
                        "username": username,
                        "discriminator": member.discriminator if hasattr(member, 'discriminator') else "",
                        "avatar": str(member.avatar.url) if member.avatar else "",
                        "source_server": str(guild.id),
                        "server_name": guild.name,
                        "joined_at": member.joined_at.timestamp() if member.joined_at else None,
                        "scraped_at": time.time()
                    }
                    
                    success, _ = self.user_manager.add_user(user_id, username, metadata)
                    if success:
                        new_members += 1
            
            logger.info(f"Processed {member_count} new members from chunk")
        
        # 运行客户端
        try:
            # 更新最后一次抓取时间
            self.last_scrape[server_id] = current_time
            
            # 尝试以自定义方式运行客户端
            logger.info("Starting Discord client...")
            try:
                # 注意：尝试明确指定为用户账户，而非机器人
                await client.start(token, bot=False)
            except TypeError:
                # 如果bot参数不被接受，则使用默认方式
                logger.warning("Bot parameter not accepted, using standard login method...")
                await client.start(token)
                
        except discord.errors.LoginFailure as e:
            logger.error(f"Invalid Discord token: {str(e)}")
            return 0, 0, f"Invalid Discord token: {str(e)}"
        except Exception as e:
            logger.error(f"Error during scraping: {str(e)}")
            return 0, 0, f"Error: {str(e)}"
        
        # 检查我们是否成功抓取了成员
        if not scrape_success and len(scraped_members) == 0:
            logger.error(f"Scraping failed: {error_message}")
            return 0, 0, error_message
        
        # 即使遇到错误，如果我们抓取到了一些成员，仍然报告成功
        logger.info(f"Scraped {len(scraped_members)} members, added {new_members} new members")
        
        # 添加服务器到配置（如果还不存在）
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