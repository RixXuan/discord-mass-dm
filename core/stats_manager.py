"""
Statistics tracking for Discord Mass DM Tool.

This module provides functionality for tracking and analyzing statistics related to:
- Message sends, failures, and responses
- Token usage and rate limiting
- User engagement and response rates
"""

import logging
import time
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import os

from utils.helpers import save_json_file, load_json_file

logger = logging.getLogger("discord_dm_tool")


class StatsManager:
    """
    Manages statistics for the DM tool.
    
    Handles tracking message statistics, token usage, and analytics.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the StatsManager.
        
        Args:
            config (Dict[str, Any]): The application configuration.
        """
        self.config = config
        self.stats_dir = Path.home() / ".discord_dm_tool" / "stats"
        self.stats_dir.mkdir(parents=True, exist_ok=True)
        
        # Session stats
        self.session_start = time.time()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize stats dictionaries
        self.message_stats = {
            "total_sent": 0,
            "successful": 0,
            "failed": 0,
            "responses": 0,
            "rate_limited": 0,
            "last_hour": 0,
            "last_24_hours": 0,
            "history": []  # List of message events
        }
        
        self.token_stats = {
            "usage": {},  # Maps token to usage count
            "rate_limits": {},  # Maps token to rate limit events
            "errors": {}  # Maps token to error events
        }
        
        self.user_stats = {
            "messaged": 0,
            "responded": 0,
            "friends_accepted": 0,
            "friends_rejected": 0
        }
        
        # Load saved stats
        self._load_stats()
        
        logger.debug("StatsManager initialized")
    
    def _load_stats(self) -> None:
        """
        Load statistics from disk.
        """
        # Load global stats
        global_stats_path = self.stats_dir / "global_stats.json"
        if global_stats_path.exists():
            try:
                global_stats = load_json_file(str(global_stats_path), default={})
                
                # Merge with current stats
                if "message_stats" in global_stats:
                    for key, value in global_stats["message_stats"].items():
                        if key != "history":  # Don't load history, which could be large
                            self.message_stats[key] = value
                
                if "user_stats" in global_stats:
                    self.user_stats.update(global_stats["user_stats"])
                
                logger.debug("Loaded global statistics")
            except Exception as e:
                logger.error(f"Failed to load global statistics: {e}")
    
    def _save_stats(self) -> None:
        """
        Save statistics to disk.
        """
        # Save global stats
        global_stats_path = self.stats_dir / "global_stats.json"
        
        try:
            global_stats = {
                "message_stats": {k: v for k, v in self.message_stats.items() if k != "history"},
                "user_stats": self.user_stats,
                "last_updated": time.time()
            }
            
            save_json_file(global_stats, str(global_stats_path))
            logger.debug("Saved global statistics")
        except Exception as e:
            logger.error(f"Failed to save global statistics: {e}")
        
        # Save session stats
        session_stats_path = self.stats_dir / f"session_{self.session_id}.json"
        
        try:
            session_stats = {
                "session_id": self.session_id,
                "session_start": self.session_start,
                "session_duration": time.time() - self.session_start,
                "message_stats": self.message_stats,
                "token_stats": self.token_stats,
                "user_stats": self.user_stats
            }
            
            save_json_file(session_stats, str(session_stats_path))
            logger.debug(f"Saved session statistics for session {self.session_id}")
        except Exception as e:
            logger.error(f"Failed to save session statistics: {e}")
    
    def track_message_sent(self, user_id: str, token: str, status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Track a message send event.
        
        Args:
            user_id (str): The Discord ID of the user.
            token (str): The token used to send the message.
            status (str): The status of the message send ("success", "failed", "rate_limited").
            metadata (Optional[Dict[str, Any]], optional): Additional metadata about the event. Defaults to None.
        """
        timestamp = time.time()
        
        # Update message stats
        self.message_stats["total_sent"] += 1
        
        if status == "success":
            self.message_stats["successful"] += 1
        elif status == "failed":
            self.message_stats["failed"] += 1
        elif status == "rate_limited":
            self.message_stats["rate_limited"] += 1
        
        # Update hourly and daily counts
        hour_ago = timestamp - 3600
        day_ago = timestamp - 86400
        
        # Clean up old history entries
        self.message_stats["history"] = [
            event for event in self.message_stats["history"]
            if event["timestamp"] > day_ago
        ]
        
        # Add new event to history
        event = {
            "timestamp": timestamp,
            "user_id": user_id,
            "token": token,
            "status": status,
            "metadata": metadata or {}
        }
        self.message_stats["history"].append(event)
        
        # Recalculate hourly and daily counts
        self.message_stats["last_hour"] = sum(
            1 for event in self.message_stats["history"]
            if event["timestamp"] > hour_ago and event["status"] == "success"
        )
        
        self.message_stats["last_24_hours"] = sum(
            1 for event in self.message_stats["history"]
            if event["status"] == "success"
        )
        
        # Update token usage
        if token in self.token_stats["usage"]:
            self.token_stats["usage"][token] += 1
        else:
            self.token_stats["usage"][token] = 1
        
        # Update token errors or rate limits if applicable
        if status == "failed":
            error_type = metadata.get("error_type", "unknown") if metadata else "unknown"
            
            if token not in self.token_stats["errors"]:
                self.token_stats["errors"][token] = {}
            
            if error_type in self.token_stats["errors"][token]:
                self.token_stats["errors"][token][error_type] += 1
            else:
                self.token_stats["errors"][token][error_type] = 1
        
        elif status == "rate_limited":
            if token in self.token_stats["rate_limits"]:
                self.token_stats["rate_limits"][token] += 1
            else:
                self.token_stats["rate_limits"][token] = 1
        
        # Update user stats
        self.user_stats["messaged"] += 1
        
        # Save stats periodically (every 10 events)
        if self.message_stats["total_sent"] % 10 == 0:
            self._save_stats()
    
    def track_message_response(self, user_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Track a message response event.
        
        Args:
            user_id (str): The Discord ID of the user who responded.
            metadata (Optional[Dict[str, Any]], optional): Additional metadata about the response. Defaults to None.
        """
        self.message_stats["responses"] += 1
        self.user_stats["responded"] += 1
        
        # Save stats
        self._save_stats()
    
    def track_friend_request(self, user_id: str, status: str, token: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Track a friend request event.
        
        Args:
            user_id (str): The Discord ID of the user.
            status (str): The status of the friend request ("sent", "accepted", "rejected", "failed").
            token (str): The token used to send the friend request.
            metadata (Optional[Dict[str, Any]], optional): Additional metadata about the event. Defaults to None.
        """
        if status == "accepted":
            self.user_stats["friends_accepted"] += 1
        elif status == "rejected":
            self.user_stats["friends_rejected"] += 1
        
        # Save stats periodically
        self._save_stats()
    
    def get_message_stats(self) -> Dict[str, Any]:
        """
        Get message statistics.
        
        Returns:
            Dict[str, Any]: Dictionary of message statistics.
        """
        # Calculate success rate
        total = self.message_stats["successful"] + self.message_stats["failed"]
        success_rate = (self.message_stats["successful"] / total * 100) if total > 0 else 0
        
        # Calculate response rate
        response_rate = (self.message_stats["responses"] / self.message_stats["successful"] * 100) if self.message_stats["successful"] > 0 else 0
        
        stats = {
            "total_sent": self.message_stats["total_sent"],
            "successful": self.message_stats["successful"],
            "failed": self.message_stats["failed"],
            "rate_limited": self.message_stats["rate_limited"],
            "responses": self.message_stats["responses"],
            "success_rate": round(success_rate, 2),
            "response_rate": round(response_rate, 2),
            "last_hour": self.message_stats["last_hour"],
            "last_24_hours": self.message_stats["last_24_hours"]
        }
        
        return stats
    
    def get_token_stats(self) -> Dict[str, Any]:
        """
        Get token usage statistics.
        
        Returns:
            Dict[str, Any]: Dictionary of token statistics.
        """
        return {
            "usage": self.token_stats["usage"],
            "rate_limits": self.token_stats["rate_limits"],
            "errors": self.token_stats["errors"]
        }
    
    def get_user_stats(self) -> Dict[str, Any]:
        """
        Get user statistics.
        
        Returns:
            Dict[str, Any]: Dictionary of user statistics.
        """
        # Calculate response rate
        response_rate = (self.user_stats["responded"] / self.user_stats["messaged"] * 100) if self.user_stats["messaged"] > 0 else 0
        
        # Calculate friend acceptance rate
        friend_requests = self.user_stats["friends_accepted"] + self.user_stats["friends_rejected"]
        acceptance_rate = (self.user_stats["friends_accepted"] / friend_requests * 100) if friend_requests > 0 else 0
        
        stats = {
            "messaged": self.user_stats["messaged"],
            "responded": self.user_stats["responded"],
            "response_rate": round(response_rate, 2),
            "friends_accepted": self.user_stats["friends_accepted"],
            "friends_rejected": self.user_stats["friends_rejected"],
            "friend_acceptance_rate": round(acceptance_rate, 2)
        }
        
        return stats
    
    def get_session_stats(self) -> Dict[str, Any]:
        """
        Get statistics for the current session.
        
        Returns:
            Dict[str, Any]: Dictionary of session statistics.
        """
        session_duration = time.time() - self.session_start
        
        stats = {
            "session_id": self.session_id,
            "session_start": datetime.fromtimestamp(self.session_start).strftime("%Y-%m-%d %H:%M:%S"),
            "session_duration": round(session_duration / 60, 2),  # In minutes
            "message_stats": self.get_message_stats(),
            "token_stats": self.get_token_stats(),
            "user_stats": self.get_user_stats()
        }
        
        return stats
    
    def reset_stats(self, session_only: bool = False) -> None:
        """
        Reset statistics.
        
        Args:
            session_only (bool, optional): If True, only reset session stats. Defaults to False.
        """
        # Reset session stats
        self.session_start = time.time()
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Initialize message stats
        self.message_stats = {
            "total_sent": 0,
            "successful": 0,
            "failed": 0,
            "responses": 0,
            "rate_limited": 0,
            "last_hour": 0,
            "last_24_hours": 0,
            "history": []
        }
        
        # Initialize token stats
        self.token_stats = {
            "usage": {},
            "rate_limits": {},
            "errors": {}
        }
        
        if not session_only:
            # Reset global stats
            self.user_stats = {
                "messaged": 0,
                "responded": 0,
                "friends_accepted": 0,
                "friends_rejected": 0
            }
            
            # Delete global stats file
            global_stats_path = self.stats_dir / "global_stats.json"
            if global_stats_path.exists():
                try:
                    os.remove(global_stats_path)
                except Exception as e:
                    logger.error(f"Failed to delete global stats file: {e}")
        
        logger.info(f"Reset {'session' if session_only else 'all'} statistics")
    
    def export_stats_to_file(self, filepath: str, include_history: bool = False) -> bool:
        """
        Export statistics to a JSON file.
        
        Args:
            filepath (str): The path to the file.
            include_history (bool, optional): Whether to include detailed message history. Defaults to False.
            
        Returns:
            bool: True if the export was successful, False otherwise.
        """
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            stats = {
                "timestamp": time.time(),
                "formatted_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "session_stats": self.get_session_stats(),
                "message_stats": self.get_message_stats(),
                "token_stats": self.get_token_stats(),
                "user_stats": self.get_user_stats()
            }
            
            if include_history:
                stats["message_history"] = self.message_stats["history"]
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(stats, f, indent=4)
            
            logger.info(f"Exported statistics to {filepath}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to export statistics to file: {e}")
            return False
    
    def get_stats_summary(self) -> str:
        """
        Get a summary of statistics as a human-readable string.
        
        Returns:
            str: Statistics summary.
        """
        message_stats = self.get_message_stats()
        user_stats = self.get_user_stats()
        session_stats = self.get_session_stats()
        
        # Format the summary
        summary = [
            "=== Discord Mass DM Tool Statistics ===",
            f"Session: {session_stats['session_id']}",
            f"Started: {session_stats['session_start']}",
            f"Duration: {session_stats['session_duration']} minutes",
            "",
            "--- Message Stats ---",
            f"Total Sent: {message_stats['total_sent']}",
            f"Successful: {message_stats['successful']}",
            f"Failed: {message_stats['failed']}",
            f"Rate Limited: {message_stats['rate_limited']}",
            f"Success Rate: {message_stats['success_rate']}%",
            f"Responses: {message_stats['responses']}",
            f"Response Rate: {message_stats['response_rate']}%",
            f"Last Hour: {message_stats['last_hour']}",
            f"Last 24 Hours: {message_stats['last_24_hours']}",
            "",
            "--- User Stats ---",
            f"Users Messaged: {user_stats['messaged']}",
            f"Users Responded: {user_stats['responded']}",
            f"Response Rate: {user_stats['response_rate']}%",
            f"Friend Requests Accepted: {user_stats['friends_accepted']}",
            f"Friend Requests Rejected: {user_stats['friends_rejected']}",
            f"Friend Acceptance Rate: {user_stats['friend_acceptance_rate']}%"
        ]
        
        return "\n".join(summary)