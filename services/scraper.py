"""
Server member scraper for Discord Mass DM Tool.

This module provides functionality for scraping members from Discord servers.
"""

import logging
import asyncio
import time
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
        
        # Check if we've scraped this server recently (within last 15 minutes)
        current_time = time.time()
        if server_id in self.last_scrape and current_time - self.last_scrape[server_id] < 900:
            time_since = int(current_time - self.last_scrape[server_id])
            logger.warning(f"Server {server_id} was scraped {time_since} seconds ago. Waiting...")
            return 0, 0, f"Server was scraped recently, please wait {900 - time_since} seconds"
        
        # Set up Discord client
        intents = discord.Intents.default()
        intents.members = True
        
        client = commands.Bot(command_prefix="!", intents=intents)
        
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
                
                # Request guild members (this triggers on_member_chunk events)
                await guild.chunk()
                
                # Wait for the members to be scraped
                await asyncio.sleep(self.scrape_timeout)
                
                # Close the client
                await client.close()
            
            except Exception as e:
                logger.error(f"Error during server scraping: {e}")
                await client.close()
        
        @client.event
        async def on_member_chunk(guild, members):
            """Called when a chunk of members is received."""
            nonlocal new_members
            
            logger.debug(f"Received member chunk with {len(members)} members")
            
            # Add each member to the list
            for member in members:
                if len(scraped_members) >= self.max_members:
                    logger.warning(f"Reached maximum member limit of {self.max_members}")
                    break
                
                # Skip bots
                if member.bot:
                    continue
                
                user_id = str(member.id)
                username = f"{member.name}"
                
                # Add to scraped members list
                scraped_members.append({
                    "user_id": user_id,
                    "username": username,
                    "discriminator": member.discriminator if hasattr(member, 'discriminator') else "",
                    "avatar": str(member.avatar.url) if member.avatar else "",
                    "joined_at": member.joined_at.timestamp() if member.joined_at else None
                })
                
                # Add to user manager if it doesn't exist
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
        
        # Run the client
        try:
            # Update last scrape time
            self.last_scrape[server_id] = current_time
            
            # Start the client
            await client.start(token)
        except discord.errors.LoginFailure:
            logger.error("Invalid Discord token")
            return 0, 0, "Invalid Discord token"
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            return 0, 0, f"Error: {str(e)}"
        
        logger.info(f"Scraped {len(scraped_members)} members, added {new_members} new members")
        
        # Add server to the configuration if it's not already there
        servers = self.scrape_settings.get("servers", [])
        if server_id not in servers:
            servers.append(server_id)
            self.scrape_settings["servers"] = servers
            self.config["scraping"] = self.scrape_settings
        
        return len(scraped_members), new_members, "Success"
    
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