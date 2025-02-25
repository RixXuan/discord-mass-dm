"""
Friend request manager for Discord Mass DM Tool.

This module provides functionality for sending friend requests and managing friend relationships.
The module is included for optional functionality, but is not required for direct messaging.
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
from core.stats_manager import StatsManager
from utils.helpers import apply_rate_limit

logger = logging.getLogger("discord_dm_tool")


class FriendManager:
    """
    Manages friend requests and friend relationships.
    
    This class provides optional functionality for sending friend requests
    and managing friend relationships, which can be used to increase success rates
    of direct messages on some Discord accounts.
    """
    
    def __init__(self, config: Dict[str, Any], token_manager: TokenManager, 
                 user_manager: UserManager, stats_manager: StatsManager):
        """
        Initialize the FriendManager.
        
        Args:
            config (Dict[str, Any]): The application configuration.
            token_manager (TokenManager): Token manager instance.
            user_manager (UserManager): User manager instance.
            stats_manager (StatsManager): Statistics manager instance.
        """
        self.config = config
        self.token_manager = token_manager
        self.user_manager = user_manager
        self.stats_manager = stats_manager
        
        # Load settings
        rate_limits = self.config.get("rate_limits", {})
        self.requests_per_minute = rate_limits.get("friend_requests_per_minute", 2)
        self.jitter_percent = rate_limits.get("jitter_percent", 20)
        self.cooldown_period = rate_limits.get("cooldown_period", 300)
        self.auto_delay = rate_limits.get("auto_delay_enabled", True)
        
        # Track currently running tasks
        self.running = False
        self.tasks = []
        self.current_progress = 0
        self.total_users = 0
        self.success_count = 0
        self.fail_count = 0
        self.rate_limit_count = 0
        
        # Cache to avoid duplicate requests
        self.sent_cache = set()
        
        logger.debug("FriendManager initialized")
    
    async def send_friend_request(self, user_id: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Send a friend request to a Discord user.
        
        Note: This is an optional function that is not required for direct messaging.
              It's included for cases where adding friends might increase DM success rates.
        
        Args:
            user_id (str): The Discord ID of the user.
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
                
                # Send friend request
                try:
                    # Discord.py doesn't have a direct method for friend requests,
                    # we need to use HTTP requests directly
                    route = discord.http.Route('PUT', '/users/@me/relationships/{user_id}', user_id=user_id)
                    await client.http.request(route, json={})
                    
                    result = {
                        "success": True,
                        "status": "Friend request sent successfully",
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
                        "status": "Cannot send friend request to this user",
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
                        "status": f"Error sending friend request: {e}",
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
    
    async def check_friend_status(self, user_id: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Check the friend status with a Discord user.
        
        Args:
            user_id (str): The Discord ID of the user.
            token (str): The Discord token to use.
            
        Returns:
            Tuple[bool, str, Optional[Dict[str, Any]]]: A tuple containing:
                - Boolean indicating if the user is a friend
                - Status message
                - Optional metadata about the result
        """
        # Set up Discord client
        intents = discord.Intents.default()
        
        client = commands.Bot(command_prefix="!", intents=intents)
        
        result = {"is_friend": False, "status": "", "metadata": None}
        
        @client.event
        async def on_ready():
            """Called when the client is ready."""
            nonlocal result
            
            try:
                # Check relationship status using HTTP API
                try:
                    route = discord.http.Route('GET', '/users/@me/relationships')
                    relationships = await client.http.request(route)
                    
                    # Find the user in relationships
                    for relationship in relationships:
                        if relationship.get("id") == user_id:
                            # Check relationship type
                            # 1 = friend, 2 = blocked, 3 = incoming request, 4 = outgoing request
                            rel_type = relationship.get("type", 0)
                            
                            if rel_type == 1:
                                result = {
                                    "is_friend": True,
                                    "status": "User is a friend",
                                    "metadata": {
                                        "user_id": user_id,
                                        "username": relationship.get("user", {}).get("username", ""),
                                        "relationship_type": "friend"
                                    }
                                }
                            elif rel_type == 3:
                                result = {
                                    "is_friend": False,
                                    "status": "Incoming friend request",
                                    "metadata": {
                                        "user_id": user_id,
                                        "username": relationship.get("user", {}).get("username", ""),
                                        "relationship_type": "incoming_request"
                                    }
                                }
                            elif rel_type == 4:
                                result = {
                                    "is_friend": False,
                                    "status": "Outgoing friend request",
                                    "metadata": {
                                        "user_id": user_id,
                                        "username": relationship.get("user", {}).get("username", ""),
                                        "relationship_type": "outgoing_request"
                                    }
                                }
                            else:
                                result = {
                                    "is_friend": False,
                                    "status": "User is not a friend",
                                    "metadata": {
                                        "user_id": user_id,
                                        "relationship_type": "none" if rel_type == 0 else "blocked" if rel_type == 2 else f"unknown_{rel_type}"
                                    }
                                }
                            break
                    else:
                        result = {
                            "is_friend": False,
                            "status": "No relationship",
                            "metadata": {
                                "user_id": user_id,
                                "relationship_type": "none"
                            }
                        }
                    
                except discord.errors.HTTPException as e:
                    result = {
                        "is_friend": False,
                        "status": f"HTTP error: {e}",
                        "metadata": {"error_type": "http_error", "error": str(e), "code": e.status if hasattr(e, 'status') else 0}
                    }
                except Exception as e:
                    result = {
                        "is_friend": False,
                        "status": f"Error checking friend status: {e}",
                        "metadata": {"error_type": "unknown", "error": str(e)}
                    }
            
            except Exception as e:
                result = {
                    "is_friend": False,
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
        
        return result["is_friend"], result["status"], result["metadata"]
    
    async def remove_friend(self, user_id: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        Remove a Discord user from friends.
        
        Args:
            user_id (str): The Discord ID of the user.
            token (str): The Discord token to use.
            
        Returns:
            Tuple[bool, str, Optional[Dict[str, Any]]]: A tuple containing:
                - Boolean indicating success or failure
                - Status message
                - Optional metadata about the result
        """
        # Set up Discord client
        intents = discord.Intents.default()
        
        client = commands.Bot(command_prefix="!", intents=intents)
        
        result = {"success": False, "status": "", "metadata": None}
        
        @client.event
        async def on_ready():
            """Called when the client is ready."""
            nonlocal result
            
            try:
                # Remove friend using HTTP API
                try:
                    route = discord.http.Route('DELETE', '/users/@me/relationships/{user_id}', user_id=user_id)
                    await client.http.request(route)
                    
                    result = {
                        "success": True,
                        "status": "Friend removed successfully",
                        "metadata": {
                            "user_id": user_id,
                            "removed_at": time.time()
                        }
                    }
                except discord.errors.NotFound:
                    result = {
                        "success": False,
                        "status": "User not found or not a friend",
                        "metadata": {"error_type": "not_found"}
                    }
                except discord.errors.HTTPException as e:
                    result = {
                        "success": False,
                        "status": f"HTTP error: {e}",
                        "metadata": {"error_type": "http_error", "error": str(e), "code": e.status if hasattr(e, 'status') else 0}
                    }
                except Exception as e:
                    result = {
                        "success": False,
                        "status": f"Error removing friend: {e}",
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
    
    async def send_bulk_friend_requests(self, user_ids: List[str],
                                      progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
                                      stop_event: Optional[asyncio.Event] = None) -> Dict[str, Any]:
        """
        Send friend requests to multiple Discord users.
        
        Args:
            user_ids (List[str]): List of Discord user IDs to send friend requests to.
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
        
        # Create stop event if not provided
        if stop_event is None:
            stop_event = asyncio.Event()
        
        # Start sending friend requests
        start_time = time.time()
        
        for i, user_id in enumerate(user_ids):
            if stop_event.is_set():
                logger.info("Bulk friend request operation stopped by user")
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
                logger.error("No tokens available for sending friend requests")
                break
            
            # Send friend request
            success, status, metadata = await self.send_friend_request(user_id, token)
            
            if success:
                self.success_count += 1
                logger.info(f"Successfully sent friend request to user '{user_id}'")
                
                # Add to sent cache
                self.sent_cache.add(user_id)
                
                # Track statistics
                self.stats_manager.track_friend_request(user_id, "sent", token, metadata)
            
            else:
                # Check if rate limited
                if metadata and metadata.get("error_type") == "rate_limited":
                    self.rate_limit_count += 1
                    retry_after = metadata.get("retry_after", self.cooldown_period)
                    
                    logger.warning(f"Rate limited. Cooling down for {retry_after} seconds")
                    
                    # Set token cooldown
                    self.token_manager.set_token_cooldown(token, retry_after)
                    
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
                    logger.error(f"Failed to send friend request to user '{user_id}': {status}")
                    
                    # Track statistics
                    self.stats_manager.track_friend_request(user_id, "failed", token, metadata)
            
            # Update progress
            self.current_progress += 1
            if progress_callback:
                progress_callback(self.current_progress, self.total_users, 
                                 self.success_count, self.fail_count)
            
            # Apply rate limiting
            if self.auto_delay and i < len(user_ids) - 1:
                apply_rate_limit(self.requests_per_minute, self.jitter_percent)
        
        # Operation complete
        end_time = time.time()
        duration = end_time - start_time
        
        self.running = False
        
        return {
            "status": "complete",
            "message": f"Bulk friend request operation completed in {duration:.2f} seconds",
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "rate_limit_count": self.rate_limit_count,
            "duration": duration
        }
    
    def start_bulk_friend_requests(self, user_ids: List[str],
                                  progress_callback: Optional[Callable[[int, int, int, int], None]] = None) -> asyncio.Task:
        """
        Start sending friend requests to multiple Discord users in the background.
        
        Args:
            user_ids (List[str]): List of Discord user IDs to send friend requests to.
            progress_callback (Optional[Callable[[int, int, int, int], None]], optional): Callback function for progress updates.
                The callback receives (current, total, success_count, fail_count). Defaults to None.
            
        Returns:
            asyncio.Task: The background task.
        """
        if self.running:
            logger.warning("A bulk friend request operation is already running")
            return None
        
        # Create stop event
        stop_event = asyncio.Event()
        
        # Create task
        task = asyncio.create_task(
            self.send_bulk_friend_requests(user_ids, progress_callback, stop_event)
        )
        
        # Store task and stop event
        self.tasks.append({
            "task": task,
            "stop_event": stop_event,
            "start_time": time.time(),
            "user_count": len(user_ids)
        })
        
        return task
    
    async def get_friends_list(self, token: str) -> Tuple[bool, List[Dict[str, Any]], Optional[str]]:
        """
        Get the list of friends for a token.
        
        Args:
            token (str): The Discord token to use.
            
        Returns:
            Tuple[bool, List[Dict[str, Any]], Optional[str]]: A tuple containing:
                - Boolean indicating success or failure
                - List of friend dictionaries
                - Error message if failed, None otherwise
        """
        # Set up Discord client
        intents = discord.Intents.default()
        
        client = commands.Bot(command_prefix="!", intents=intents)
        
        result = {"success": False, "friends": [], "error": None}
        
        @client.event
        async def on_ready():
            """Called when the client is ready."""
            nonlocal result
            
            try:
                # Get friends using HTTP API
                try:
                    route = discord.http.Route('GET', '/users/@me/relationships')
                    relationships = await client.http.request(route)
                    
                    # Filter friends (type 1 = friend)
                    friends = []
                    for rel in relationships:
                        if rel.get("type") == 1:
                            user_data = rel.get("user", {})
                            friend = {
                                "user_id": user_data.get("id"),
                                "username": user_data.get("username"),
                                "discriminator": user_data.get("discriminator", ""),
                                "avatar": user_data.get("avatar"),
                                "relationship_id": rel.get("id")
                            }
                            friends.append(friend)
                    
                    result = {
                        "success": True,
                        "friends": friends,
                        "error": None
                    }
                    
                except discord.errors.HTTPException as e:
                    result = {
                        "success": False,
                        "friends": [],
                        "error": f"HTTP error: {e}"
                    }
                except Exception as e:
                    result = {
                        "success": False,
                        "friends": [],
                        "error": f"Error getting friends: {e}"
                    }
            
            except Exception as e:
                result = {
                    "success": False,
                    "friends": [],
                    "error": f"Unexpected error: {e}"
                }
            
            finally:
                await client.close()
        
        # Start the client
        try:
            await client.start(token)
        except discord.errors.LoginFailure:
            return False, [], "Invalid Discord token"
        except Exception as e:
            return False, [], f"Error during login: {e}"
        
        return result["success"], result["friends"], result["error"]
    
    def stop_all_tasks(self) -> int:
        """
        Stop all running friend request tasks.
        
        Returns:
            int: Number of tasks stopped.
        """
        stopped_count = 0
        
        for task_info in self.tasks:
            if not task_info["task"].done():
                task_info["stop_event"].set()
                stopped_count += 1
        
        self.running = False
        logger.info(f"Stopped {stopped_count} friend request tasks")
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
        Check if a bulk friend request operation is currently running.
        
        Returns:
            bool: True if an operation is running, False otherwise.
        """
        return self.running