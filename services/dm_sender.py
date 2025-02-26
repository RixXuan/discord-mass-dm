"""
Direct message sender for Discord Mass DM Tool.

This module provides functionality for sending direct messages to Discord users.
"""

import logging
import time
import asyncio
import random
from typing import Dict, List, Optional, Any, Tuple, Set, Callable

import discord
from discord.ext import commands

import aiohttp

from core.token_manager import TokenManager
from core.user_manager import UserManager
from core.message_manager import MessageManager
from core.stats_manager import StatsManager
from utils.helpers import apply_rate_limit

logger = logging.getLogger("discord_dm_tool")


class DMSender:
    """
    Sends direct messages to Discord users.
    
    Uses Discord tokens to send DMs to users based on configured templates and settings.
    """
    
    def __init__(self, config: Dict[str, Any], token_manager: TokenManager, 
                 user_manager: UserManager, message_manager: MessageManager,
                 stats_manager: StatsManager):
        """
        Initialize the DMSender.
        
        Args:
            config (Dict[str, Any]): The application configuration.
            token_manager (TokenManager): Token manager instance.
            user_manager (UserManager): User manager instance.
            message_manager (MessageManager): Message template manager instance.
            stats_manager (StatsManager): Statistics manager instance.
        """
        self.config = config
        self.token_manager = token_manager
        self.user_manager = user_manager
        self.message_manager = message_manager
        self.stats_manager = stats_manager
        
        # Load settings
        rate_limits = self.config.get("rate_limits", {})
        self.messages_per_minute = rate_limits.get("messages_per_minute", 5)
        self.jitter_percent = rate_limits.get("jitter_percent", 20)
        self.cooldown_period = rate_limits.get("cooldown_period", 300)
        self.auto_delay = rate_limits.get("auto_delay_enabled", True)
        
        dm_settings = self.config.get("dm_settings", {})
        self.retry_failed = dm_settings.get("retry_failed", True)
        self.max_retries = dm_settings.get("max_retries", 3)
        self.track_stats = dm_settings.get("track_statistics", True)
        
        # Track currently running tasks
        self.running = False
        self.tasks = []
        self.current_progress = 0
        self.total_users = 0
        self.success_count = 0
        self.fail_count = 0
        self.rate_limit_count = 0
        
        # Cache to avoid duplicate sends
        self.sent_cache = set()
        
        logger.debug("DMSender initialized")
    
    async def send_dm(self, user_id: str, message: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Send a direct message to a Discord user using direct API call.
        
        Args:
            user_id (str): The Discord ID of the user to message.
            message (str): The message content to send.
            token (str): The Discord token to use.
            
        Returns:
            Tuple[bool, str, Optional[Dict[str, Any]]]: A tuple containing:
                - Boolean indicating success or failure
                - Status message
                - Optional metadata about the result
        """
        logger.info(f"Attempting to send DM to user {user_id} via API")
        
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # 步骤1: 创建DM通道
                create_dm_url = 'https://discord.com/api/v9/users/@me/channels'
                create_dm_payload = {'recipient_id': user_id}
                
                async with session.post(
                    create_dm_url, 
                    headers=headers, 
                    json=create_dm_payload
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Failed to create DM channel: {error_text}")
                        return False, f"Failed to create DM channel: Status {response.status}", {
                            "error_type": "channel_creation_failed",
                            "status_code": response.status
                        }
                    
                    channel_data = await response.json()
                    channel_id = channel_data.get('id')
                    
                    if not channel_id:
                        return False, "Could not get DM channel ID", {"error_type": "no_channel_id"}
                    
                    # 步骤2: 发送消息
                    send_message_url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
                    message_payload = {'content': message}
                    
                    async with session.post(
                        send_message_url, 
                        headers=headers, 
                        json=message_payload
                    ) as msg_response:
                        if msg_response.status == 200 or msg_response.status == 201:
                            message_data = await msg_response.json()
                            return True, "Message sent successfully", {
                                "message_id": message_data.get('id'),
                                "channel_id": channel_id,
                                "timestamp": message_data.get('timestamp'),
                                "user_id": user_id
                            }
                        elif msg_response.status == 429:
                            # 速率限制
                            limit_data = await msg_response.json()
                            retry_after = limit_data.get('retry_after', self.cooldown_period)
                            return False, f"Rate limited. Try again in {retry_after} seconds", {
                                "error_type": "rate_limited",
                                "retry_after": retry_after
                            }
                        else:
                            error_text = await msg_response.text()
                            return False, f"Failed to send message: {error_text}", {
                                "error_type": "message_send_failed",
                                "status_code": msg_response.status
                            }
        
        except Exception as e:
            logger.error(f"Error in send_dm: {str(e)}")
            return False, f"Error: {str(e)}", {"error_type": "exception", "error": str(e)}
    
    async def send_bulk_dms(self, template_id: str, user_ids: List[str], 
                            variables: Dict[str, str] = None,
                            progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
                            stop_event: Optional[asyncio.Event] = None) -> Dict[str, Any]:
        """
        Send direct messages to multiple Discord users.
        
        Args:
            template_id (str): The ID of the message template to use.
            user_ids (List[str]): List of Discord user IDs to message.
            variables (Dict[str, str], optional): Variables to use in the message template. Defaults to None.
            progress_callback (Optional[Callable[[int, int, int, int], None]], optional): Callback function for progress updates.
                The callback receives (current, total, success_count, fail_count). Defaults to None.
            stop_event (Optional[asyncio.Event], optional): Event to signal stopping the operation. Defaults to None.
            
        Returns:
            Dict[str, Any]: Dictionary with operation statistics.
        """
        # Reset counters
        self.current_progress = 0
        self.total_users = len(user_ids)
        self.success_count = 0
        self.fail_count = 0
        self.rate_limit_count = 0
        self.sent_cache = set()
        self.running = True
        
        # Get template
        template = self.message_manager.get_template(template_id)
        if not template:
            logger.error(f"Template with ID '{template_id}' not found")
            self.running = False
            return {
                "status": "error",
                "message": "Template not found",
                "success_count": 0,
                "fail_count": 0,
                "rate_limit_count": 0
            }
        
        variables = variables or {}
        
        # Validate template variables
        is_valid, missing_vars = self.message_manager.validate_template_variables(template_id, variables)
        if not is_valid:
            logger.error(f"Missing template variables: {', '.join(missing_vars)}")
            self.running = False
            return {
                "status": "error",
                "message": f"Missing template variables: {', '.join(missing_vars)}",
                "success_count": 0,
                "fail_count": 0,
                "rate_limit_count": 0
            }
        
        # Format base message
        base_message = self.message_manager.format_template(template_id, variables)
        
        # Create stop event if not provided
        if stop_event is None:
            stop_event = asyncio.Event()
        
        # Start sending messages
        start_time = time.time()
        
        for i, user_id in enumerate(user_ids):
            if stop_event.is_set():
                logger.info("Bulk DM operation stopped by user")
                break
            
            # Get user
            user = self.user_manager.get_user_by_discord_id(user_id)
            if not user:
                logger.warning(f"User with ID '{user_id}' not found in user manager")
                self.fail_count += 1
                self.current_progress += 1
                
                if progress_callback:
                    progress_callback(self.current_progress, self.total_users, 
                                     self.success_count, self.fail_count)
                continue
            
            # Check if already sent in this session
            if user_id in self.sent_cache:
                logger.debug(f"Skipping user '{user_id}' (already sent in this session)")
                self.current_progress += 1
                
                if progress_callback:
                    progress_callback(self.current_progress, self.total_users, 
                                     self.success_count, self.fail_count)
                continue
            
            # Get token
            token = self.token_manager.get_next_token()
            if token is None:
                logger.error("No tokens available for sending DMs")
                break
            
            # Personalize message if needed
            message = base_message
            if "{username}" in message and "username" not in variables:
                # Use username from user manager
                username = user.get("username", "user")
                message = message.replace("{username}", username)
            
            # Send message
            success, status, metadata = await self.send_dm(user_id, message, token)
            
            if success:
                self.success_count += 1
                logger.info(f"Successfully sent DM to user '{user_id}'")
                
                # Mark as messaged
                self.user_manager.mark_user_as_messaged(user_id, "sent", metadata)
                
                # Add to sent cache
                self.sent_cache.add(user_id)
                
                # Track statistics
                if self.track_stats:
                    self.stats_manager.track_message_sent(user_id, token, "success", metadata)
            
            else:
                # Check if rate limited
                if metadata and metadata.get("error_type") == "rate_limited":
                    self.rate_limit_count += 1
                    retry_after = metadata.get("retry_after", self.cooldown_period)
                    
                    logger.warning(f"Rate limited. Cooling down for {retry_after} seconds")
                    
                    # Set token cooldown
                    self.token_manager.set_token_cooldown(token, retry_after)
                    
                    # Track statistics
                    if self.track_stats:
                        self.stats_manager.track_message_sent(user_id, token, "rate_limited", metadata)
                    
                    # Sleep for a while
                    if stop_event:
                        try:
                            # Sleep but allow for interruption
                            await asyncio.wait_for(stop_event.wait(), timeout=min(retry_after, 5))
                            if stop_event.is_set():
                                break
                        except asyncio.TimeoutError:
                            pass
                    else:
                        await asyncio.sleep(min(retry_after, 5))
                
                else:
                    # Other failure
                    self.fail_count += 1
                    logger.error(f"Failed to send DM to user '{user_id}': {status}")
                    
                    # Mark as failed
                    self.user_manager.mark_user_as_messaged(user_id, "failed", metadata)
                    
                    # Track statistics
                    if self.track_stats:
                        self.stats_manager.track_message_sent(user_id, token, "failed", metadata)
            
            # Update progress
            self.current_progress += 1
            if progress_callback:
                progress_callback(self.current_progress, self.total_users, 
                                 self.success_count, self.fail_count)
            
            # Apply rate limiting
            if self.auto_delay and i < len(user_ids) - 1:
                apply_rate_limit(self.messages_per_minute, self.jitter_percent)
        
        # Operation complete
        end_time = time.time()
        duration = end_time - start_time
        
        self.running = False
        
        return {
            "status": "complete",
            "message": f"Bulk DM operation completed in {duration:.2f} seconds",
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "rate_limit_count": self.rate_limit_count,
            "duration": duration
        }
    
    def start_bulk_dms(self, template_id: str, user_ids: List[str], 
                      variables: Dict[str, str] = None,
                      progress_callback: Optional[Callable[[int, int, int, int], None]] = None) -> asyncio.Task:
        """
        Start sending direct messages to multiple Discord users in the background.
        
        Args:
            template_id (str): The ID of the message template to use.
            user_ids (List[str]): List of Discord user IDs to message.
            variables (Dict[str, str], optional): Variables to use in the message template. Defaults to None.
            progress_callback (Optional[Callable[[int, int, int, int], None]], optional): Callback function for progress updates.
                The callback receives (current, total, success_count, fail_count). Defaults to None.
            
        Returns:
            asyncio.Task: The background task.
        """
        if self.running:
            logger.warning("A bulk DM operation is already running")
            return None
        
        # Create stop event
        stop_event = asyncio.Event()
        
        # Create task
        task = asyncio.create_task(
            self.send_bulk_dms(template_id, user_ids, variables, progress_callback, stop_event)
        )
        
        # Store task and stop event
        self.tasks.append({
            "task": task,
            "stop_event": stop_event,
            "start_time": time.time(),
            "user_count": len(user_ids)
        })
        
        return task
    
    def stop_all_tasks(self) -> int:
        """
        Stop all running DM tasks.
        
        Returns:
            int: Number of tasks stopped.
        """
        stopped_count = 0
        
        for task_info in self.tasks:
            if not task_info["task"].done():
                task_info["stop_event"].set()
                stopped_count += 1
        
        self.running = False
        logger.info(f"Stopped {stopped_count} DM tasks")
        return stopped_count
    
    def get_running_tasks(self) -> List[Dict[str, Any]]:
        """
        Get information about currently running tasks.
        
        Returns:
            List[Dict[str, Any]]: List of task information dictionaries.
        """
        # Clean up completed tasks
        self.tasks = [t for t in self.tasks if not t["task"].done()]
        
        return [
            {
                "start_time": task["start_time"],
                "duration": time.time() - task["start_time"],
                "user_count": task["user_count"],
                "progress": self.current_progress,
                "success_count": self.success_count,
                "fail_count": self.fail_count,
                "rate_limit_count": self.rate_limit_count
            }
            for task in self.tasks
        ]
    
    def is_running(self) -> bool:
        """
        Check if a bulk DM operation is currently running.
        
        Returns:
            bool: True if an operation is running, False otherwise.
        """
        return self.running