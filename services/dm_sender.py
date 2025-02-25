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
        Send a direct message to a Discord user.
        
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
        # Set up Discord client
        intents = discord.Intents.default()
        intents.dm_messages = True
        
        client = commands.Bot(command_prefix="!", intents=intents)
        
        result = {"success": False, "status": "", "metadata": None}
        
        @client.event
        async def on_ready():
            """Called when the client is ready."""
            nonlocal result
            
            try:
                # Get the user
                user = None
                try:
                    user = await client.fetch_user(int(user_id))
                except discord.errors.NotFound:
                    result = {
                        "success": False,
                        "status": "User not found",
                        "metadata": {"error_type": "user_not_found"}
                    }
                    await client.close()
                    return
                except Exception as e:
                    result = {
                        "success": False,
                        "status": f"Error fetching user: {e}",
                        "metadata": {"error_type": "fetch_error", "error": str(e)}
                    }
                    await client.close()
                    return
                
                if not user:
                    result = {
                        "success": False,
                        "status": "User not found",
                        "metadata": {"error_type": "user_not_found"}
                    }
                    await client.close()
                    return
                
                # Create a DM channel and send message
                try:
                    dm_channel = await user.create_dm()
                    await dm_channel.send(message)
                    
                    result = {
                        "success": True,
                        "status": "Message sent successfully",
                        "metadata": {
                            "user_id": user_id,
                            "username": user.name,
                            "discriminator": user.discriminator if hasattr(user, 'discriminator') else "",
                            "sent_at": time.time()
                        }
                    }
                except discord.errors.Forbidden:
                    result = {
                        "success": False,
                        "status": "Cannot send message to this user",
                        "metadata": {"error_type": "forbidden"}
                    }
                except discord.errors.HTTPException as e:
                    # Check if rate limited
                    if e.status == 429:
                        wait_time = e.retry_after if hasattr(e, 'retry_after') else self.cooldown_period
                        
                        result = {
                            "success": False,
                            "status": f"Rate limited. Try again in {wait_time} seconds",
                            "metadata": {"error_type": "rate_limited", "retry_after": wait_time}
                        }
                    else:
                        result = {
                            "success": False,
                            "status": f"HTTP error: {e}",
                            "metadata": {"error_type": "http_error", "error": str(e), "code": e.status}
                        }
                except Exception as e:
                    result = {
                        "success": False,
                        "status": f"Error sending message: {e}",
                        "metadata": {"error_type": "unknown", "error": str(e)}
                    }
            
            except Exception as e:
                result = {
                    "success": False,
                    "status": f"Unexpected error: {e}",
                    "metadata": {"error_type": "unexpected", "error": str(e)}
                }
            
            finally:
                await client.close()
        
        # Start the client
        try:
            await client.start(token)
        except discord.errors.LoginFailure:
            return False, "Invalid Discord token", {"error_type": "invalid_token"}
        except Exception as e:
            return False, f"Error during login: {e}", {"error_type": "login_error", "error": str(e)}
        
        return result["success"], result["status"], result["metadata"]
    
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