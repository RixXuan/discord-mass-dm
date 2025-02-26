"""
Server member scraper for Discord Mass DM Tool.

This module provides functionality for scraping members from Discord servers.
"""

import logging
import asyncio
import time
import aiohttp
import json
from typing import Dict, List, Optional, Any, Tuple, Set

from core.token_manager import TokenManager
from core.user_manager import UserManager
from utils.helpers import is_snowflake

logger = logging.getLogger("discord_dm_tool")


class MemberScraper:
    """
    Scrapes members from Discord servers using direct API calls.
    """
    
    def __init__(self, config: Dict[str, Any], token_manager: TokenManager, user_manager: UserManager):
        """Initialize the MemberScraper."""
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
        """Scrape members from a Discord server using direct API calls."""
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
        
        # Check if we've scraped this server recently
        current_time = time.time()
        if server_id in self.last_scrape and current_time - self.last_scrape[server_id] < 900:
            time_since = int(current_time - self.last_scrape[server_id])
            logger.warning(f"Server {server_id} was scraped {time_since} seconds ago. Waiting...")
            return 0, 0, f"Server was scraped recently, please wait {900 - time_since} seconds"
        
        # 更新最后抓取时间
        self.last_scrape[server_id] = current_time
        
        # 利用直接 API 获取成员
        scraped_members = []
        new_members = 0
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # 验证令牌
                logger.info("Validating token...")
                async with session.get('https://discord.com/api/v9/users/@me', headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Token validation failed: {error_text}")
                        return 0, 0, "Invalid token"
                    
                    user_data = await response.json()
                    logger.info(f"Authenticated as: {user_data.get('username')}")
                
                # 验证服务器访问权限
                logger.info(f"Verifying server access...")
                async with session.get(f'https://discord.com/api/v9/guilds/{server_id}', headers=headers) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Server access failed: {error_text}")
                        return 0, 0, "Cannot access server"
                    
                    guild_data = await response.json()
                    logger.info(f"Access verified to server: {guild_data.get('name')}")
                
                # 尝试使用各种端点获取成员
                logger.info("Attempting to fetch members...")
                
                # 方法 1: 直接获取在线成员 (通常权限较低)
                logger.info("Method 1: Trying widget endpoint...")
                async with session.get(f'https://discord.com/api/v9/guilds/{server_id}/widget.json', headers=headers) as response:
                    if response.status == 200:
                        widget_data = await response.json()
                        if 'members' in widget_data:
                            logger.info(f"Found {len(widget_data['members'])} members via widget")
                            for member in widget_data['members']:
                                if len(scraped_members) >= self.max_members:
                                    break
                                    
                                user_id = member.get('id')
                                if not user_id:
                                    continue
                                
                                username = member.get('username', 'Unknown')
                                
                                scraped_members.append({
                                    "user_id": user_id,
                                    "username": username,
                                    "discriminator": "",
                                    "avatar": member.get('avatar_url', ''),
                                    "status": member.get('status', '')
                                })
                                
                                # 添加到用户管理器
                                if not self.user_manager.get_user_by_discord_id(user_id):
                                    metadata = {
                                        "username": username,
                                        "source_server": server_id,
                                        "server_name": guild_data.get('name', 'Unknown'),
                                        "scraped_at": time.time()
                                    }
                                    
                                    success, _ = self.user_manager.add_user(user_id, username, metadata)
                                    if success:
                                        new_members += 1
                
                # 方法 2: 尝试获取成员列表 (需要较高权限)
                if len(scraped_members) < self.max_members:
                    logger.info("Method 2: Trying members endpoint...")
                    
                    # 使用分页
                    after = "0"
                    while len(scraped_members) < self.max_members:
                        endpoint = f'https://discord.com/api/v9/guilds/{server_id}/members?limit=1000'
                        if after and after != "0":
                            endpoint += f'&after={after}'
                            
                        async with session.get(endpoint, headers=headers) as response:
                            if response.status != 200:
                                logger.warning(f"Members endpoint failed: Status {response.status}")
                                break
                            
                            members_data = await response.json()
                            if not members_data or len(members_data) == 0:
                                break
                                
                            logger.info(f"Fetched {len(members_data)} members")
                            
                            if len(members_data) > 0:
                                # 更新after参数为最后一个用户的ID
                                after = members_data[-1].get('user', {}).get('id', None)
                            
                            for member_data in members_data:
                                user_data = member_data.get('user', {})
                                if user_data.get('bot', False):
                                    continue
                                    
                                user_id = user_data.get('id')
                                if not user_id or any(m.get("user_id") == user_id for m in scraped_members):
                                    continue
                                    
                                username = user_data.get('username', 'Unknown')
                                
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
                            
                            # 避免达到速率限制
                            await asyncio.sleep(0.5)
                
                # 方法 3: 尝试通过搜索或其他端点
                if len(scraped_members) < 10:  # 如果前两个方法收获不多
                    logger.info("Method 3: Trying channel messages...")
                    
                    # 获取服务器频道
                    async with session.get(f'https://discord.com/api/v9/guilds/{server_id}/channels', headers=headers) as response:
                        if response.status == 200:
                            channels = await response.json()
                            
                            # 筛选文本频道
                            text_channels = [c for c in channels if c.get('type') == 0]
                            logger.info(f"Found {len(text_channels)} text channels")
                            
                            # 从每个频道获取最近消息
                            for channel in text_channels[:5]:  # 限制为前5个频道
                                channel_id = channel.get('id')
                                
                                async with session.get(f'https://discord.com/api/v9/channels/{channel_id}/messages?limit=100', headers=headers) as msg_response:
                                    if msg_response.status == 200:
                                        messages = await msg_response.json()
                                        logger.info(f"Found {len(messages)} messages in channel {channel.get('name')}")
                                        
                                        for message in messages:
                                            author = message.get('author', {})
                                            if author.get('bot', False):
                                                continue
                                                
                                            user_id = author.get('id')
                                            if not user_id or any(m.get("user_id") == user_id for m in scraped_members):
                                                continue
                                                
                                            username = author.get('username', 'Unknown')
                                            
                                            scraped_members.append({
                                                "user_id": user_id,
                                                "username": username,
                                                "discriminator": author.get('discriminator', ''),
                                                "avatar": author.get('avatar', ''),
                                                "message_time": message.get('timestamp')
                                            })
                                            
                                            # 添加到用户管理器
                                            if not self.user_manager.get_user_by_discord_id(user_id):
                                                metadata = {
                                                    "username": username,
                                                    "discriminator": author.get('discriminator', ''),
                                                    "avatar": author.get('avatar', ''),
                                                    "source_server": server_id,
                                                    "server_name": guild_data.get('name', 'Unknown'),
                                                    "message_time": message.get('timestamp'),
                                                    "scraped_at": time.time()
                                                }
                                                
                                                success, _ = self.user_manager.add_user(user_id, username, metadata)
                                                if success:
                                                    new_members += 1
                                            
                                            if len(scraped_members) >= self.max_members:
                                                break
                                                
                                        # 避免达到速率限制
                                        await asyncio.sleep(1)
                
                # 总结结果
                member_count = len(scraped_members)
                logger.info(f"Total scraped: {member_count} members")
        
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return 0, 0, f"Error: {str(e)}"
        
        # 添加服务器到配置（如果还不存在）
        servers = self.scrape_settings.get("servers", [])
        if server_id not in servers:
            servers.append(server_id)
            self.scrape_settings["servers"] = servers
            self.config["scraping"] = self.scrape_settings
        
        if len(scraped_members) > 0:
            return len(scraped_members), new_members, "Success"
        else:
            return 0, 0, "No members found (possibly no permission to view members)"
    
    def get_scrape_history(self) -> Dict[str, Dict[str, Any]]:
        """Get scraping history."""
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