"""
User management for Discord Mass DM Tool.

This module provides functionality for managing Discord users, including:
- Adding, removing, and managing user IDs
- Tracking message status per user
- Filtering and organizing users into groups
"""

import logging
import time
import json
import os
from typing import Dict, List, Set, Optional, Any, Tuple
from pathlib import Path

from utils.helpers import is_snowflake, generate_random_id, save_json_file, load_json_file

logger = logging.getLogger("discord_dm_tool")


class UserManager:
    """
    Manages Discord users for the DM tool.
    
    Handles user storage, user filtering, and tracking message status.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the UserManager.
        
        Args:
            config (Dict[str, Any]): The application configuration.
        """
        self.config = config
        self.users = self._load_users_from_config()
        self.user_groups = self._load_user_groups()
        self.message_status = {}  # Maps user_id to message status (sent, failed, etc.)
        
        # Set of user IDs that have been messaged
        self.messaged_users = set()
        
        # Load message history if available
        self._load_message_history()
        
        logger.debug(f"UserManager initialized with {len(self.users)} users and {len(self.user_groups)} groups")
    
    def _load_users_from_config(self) -> List[Dict[str, Any]]:
        """
        Load users from the configuration.
        
        Returns:
            List[Dict[str, Any]]: List of user dictionaries.
        """
        users = self.config.get("users", [])
        
        # Convert simple user IDs to user dictionaries if needed
        for i, user in enumerate(users):
            if isinstance(user, str):
                users[i] = {
                    "id": generate_random_id(),
                    "user_id": user,
                    "username": f"User {i+1}",
                    "added_at": time.time(),
                    "metadata": {}
                }
        
        return users
    
    def _load_user_groups(self) -> Dict[str, List[str]]:
        """
        Load user groups from the configuration.
        
        Returns:
            Dict[str, List[str]]: Dictionary mapping group names to lists of user IDs.
        """
        return self.config.get("user_groups", {})
    
    def _load_message_history(self) -> None:
        """
        Load message history from disk.
        """
        history_path = Path.home() / ".discord_dm_tool" / "message_history.json"
        if not history_path.exists():
            return
        
        try:
            history = load_json_file(str(history_path), default={})
            self.messaged_users = set(history.get("messaged_users", []))
            self.message_status = history.get("message_status", {})
            
            logger.debug(f"Loaded message history with {len(self.messaged_users)} messaged users")
        except Exception as e:
            logger.error(f"Failed to load message history: {e}")
    
    def _save_message_history(self) -> None:
        """
        Save message history to disk.
        """
        history_path = Path.home() / ".discord_dm_tool" / "message_history.json"
        history_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            history = {
                "messaged_users": list(self.messaged_users),
                "message_status": self.message_status,
                "last_updated": time.time()
            }
            
            save_json_file(history, str(history_path))
            logger.debug("Saved message history")
        except Exception as e:
            logger.error(f"Failed to save message history: {e}")
    
    def add_user(self, user_id: str, username: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, Optional[str]]:
        """
        Add a new user to the manager.
        
        Args:
            user_id (str): The Discord user ID to add.
            username (Optional[str], optional): The username. Defaults to None.
            metadata (Optional[Dict[str, Any]], optional): Additional user metadata. Defaults to None.
            
        Returns:
            Tuple[bool, Optional[str]]: A tuple containing:
                - Boolean indicating success or failure
                - Error message if validation failed, None otherwise
        """
        # Validate user ID
        if not is_snowflake(user_id):
            return False, "Invalid Discord user ID format"
        
        # Check if user already exists
        if any(u["user_id"] == user_id for u in self.users):
            return False, "User already exists"
        
        # Set default username if not provided
        if not username:
            username = f"User {len(self.users) + 1}"
        
        # Create user entry
        user_entry = {
            "id": generate_random_id(),
            "user_id": user_id,
            "username": username,
            "added_at": time.time(),
            "metadata": metadata or {}
        }
        
        # Add to user list
        self.users.append(user_entry)
        
        # Update configuration
        self.config["users"] = self.users
        
        logger.info(f"Added user with ID '{user_id}' and username '{username}'")
        return True, None
    
    def remove_user(self, internal_id: str) -> bool:
        """
        Remove a user from the manager.
        
        Args:
            internal_id (str): The internal ID of the user to remove.
            
        Returns:
            bool: True if the user was removed, False otherwise.
        """
        original_length = len(self.users)
        self.users = [u for u in self.users if u["id"] != internal_id]
        
        # Check if a user was removed
        if len(self.users) < original_length:
            # Update configuration
            self.config["users"] = self.users
            logger.info(f"Removed user with internal ID '{internal_id}'")
            return True
        
        logger.warning(f"No user found with internal ID '{internal_id}'")
        return False
    
    def get_user(self, internal_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by its internal ID.
        
        Args:
            internal_id (str): The internal ID of the user to get.
            
        Returns:
            Optional[Dict[str, Any]]: The user dictionary, or None if not found.
        """
        for user in self.users:
            if user["id"] == internal_id:
                return user
        return None
    
    def get_user_by_discord_id(self, discord_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by its Discord ID.
        
        Args:
            discord_id (str): The Discord ID of the user to get.
            
        Returns:
            Optional[Dict[str, Any]]: The user dictionary, or None if not found.
        """
        for user in self.users:
            if user["user_id"] == discord_id:
                return user
        return None
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Get all users in the manager.
        
        Returns:
            List[Dict[str, Any]]: List of all user dictionaries.
        """
        return self.users.copy()
    
    def get_user_count(self) -> int:
        """
        Get the number of users in the manager.
        
        Returns:
            int: The number of users.
        """
        return len(self.users)
    
    def get_unmessaged_users(self) -> List[Dict[str, Any]]:
        """
        Get users that haven't been messaged yet.
        
        Returns:
            List[Dict[str, Any]]: List of unmessaged user dictionaries.
        """
        return [u for u in self.users if u["user_id"] not in self.messaged_users]
    
    def mark_user_as_messaged(self, discord_id: str, status: str = "sent", metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Mark a user as messaged.
        
        Args:
            discord_id (str): The Discord ID of the user.
            status (str, optional): The message status. Defaults to "sent".
            metadata (Optional[Dict[str, Any]], optional): Additional status metadata. Defaults to None.
        """
        self.messaged_users.add(discord_id)
        
        status_entry = {
            "status": status,
            "timestamp": time.time(),
            "metadata": metadata or {}
        }
        
        self.message_status[discord_id] = status_entry
        self._save_message_history()
    
    def reset_message_status(self, discord_id: Optional[str] = None) -> None:
        """
        Reset message status for a user or all users.
        
        Args:
            discord_id (Optional[str], optional): The Discord ID of the user to reset,
                or None to reset all users. Defaults to None.
        """
        if discord_id:
            if discord_id in self.messaged_users:
                self.messaged_users.remove(discord_id)
            
            if discord_id in self.message_status:
                del self.message_status[discord_id]
            
            logger.info(f"Reset message status for user '{discord_id}'")
        else:
            self.messaged_users.clear()
            self.message_status.clear()
            logger.info("Reset message status for all users")
        
        self._save_message_history()
    
    def add_user_group(self, group_name: str, user_ids: List[str] = None) -> bool:
        """
        Add a new user group.
        
        Args:
            group_name (str): The name of the group.
            user_ids (List[str], optional): List of user IDs to add to the group. Defaults to None.
            
        Returns:
            bool: True if the group was added, False if it already exists.
        """
        if group_name in self.user_groups:
            return False
        
        self.user_groups[group_name] = user_ids or []
        
        # Update configuration
        self.config["user_groups"] = self.user_groups
        
        logger.info(f"Added user group '{group_name}' with {len(user_ids or [])} users")
        return True
    
    def remove_user_group(self, group_name: str) -> bool:
        """
        Remove a user group.
        
        Args:
            group_name (str): The name of the group to remove.
            
        Returns:
            bool: True if the group was removed, False if it doesn't exist.
        """
        if group_name not in self.user_groups:
            return False
        
        del self.user_groups[group_name]
        
        # Update configuration
        self.config["user_groups"] = self.user_groups
        
        logger.info(f"Removed user group '{group_name}'")
        return True
    
    def add_user_to_group(self, group_name: str, discord_id: str) -> bool:
        """
        Add a user to a group.
        
        Args:
            group_name (str): The name of the group.
            discord_id (str): The Discord ID of the user to add.
            
        Returns:
            bool: True if the user was added, False otherwise.
        """
        if group_name not in self.user_groups:
            return False
        
        if discord_id in self.user_groups[group_name]:
            return False
        
        self.user_groups[group_name].append(discord_id)
        
        # Update configuration
        self.config["user_groups"] = self.user_groups
        
        logger.debug(f"Added user '{discord_id}' to group '{group_name}'")
        return True
    
    def remove_user_from_group(self, group_name: str, discord_id: str) -> bool:
        """
        Remove a user from a group.
        
        Args:
            group_name (str): The name of the group.
            discord_id (str): The Discord ID of the user to remove.
            
        Returns:
            bool: True if the user was removed, False otherwise.
        """
        if group_name not in self.user_groups:
            return False
        
        if discord_id not in self.user_groups[group_name]:
            return False
        
        self.user_groups[group_name].remove(discord_id)
        
        # Update configuration
        self.config["user_groups"] = self.user_groups
        
        logger.debug(f"Removed user '{discord_id}' from group '{group_name}'")
        return True
    
    def get_user_groups(self) -> Dict[str, List[str]]:
        """
        Get all user groups.
        
        Returns:
            Dict[str, List[str]]: Dictionary mapping group names to lists of user IDs.
        """
        return self.user_groups.copy()
    
    def get_users_in_group(self, group_name: str) -> List[Dict[str, Any]]:
        """
        Get all users in a group.
        
        Args:
            group_name (str): The name of the group.
            
        Returns:
            List[Dict[str, Any]]: List of user dictionaries in the group.
        """
        if group_name not in self.user_groups:
            return []
        
        group_user_ids = self.user_groups[group_name]
        return [u for u in self.users if u["user_id"] in group_user_ids]
    
    def import_users_from_file(self, filepath: str) -> Tuple[int, int]:
        """
        Import users from a file.
        
        The file should contain one user ID per line, or be in JSON format with
        user objects that have at least a "user_id" field.
        
        Args:
            filepath (str): The path to the file.
            
        Returns:
            Tuple[int, int]: A tuple containing:
                - Number of successfully imported users
                - Number of failed imports
        """
        success_count = 0
        fail_count = 0
        
        try:
            file_ext = os.path.splitext(filepath)[1].lower()
            
            if file_ext == '.json':
                # JSON file
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    for user in data:
                        if isinstance(user, str):
                            # Simple user ID
                            success, _ = self.add_user(user)
                        elif isinstance(user, dict) and "user_id" in user:
                            # User dictionary
                            success, _ = self.add_user(
                                user["user_id"],
                                username=user.get("username"),
                                metadata=user.get("metadata")
                            )
                        else:
                            success = False
                        
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1
            else:
                # Text file with one user ID per line
                with open(filepath, 'r', encoding='utf-8') as f:
                    for line in f:
                        user_id = line.strip()
                        if not user_id:
                            continue
                        
                        success, _ = self.add_user(user_id)
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1
            
            logger.info(f"Imported {success_count} users from file, {fail_count} failed")
            return success_count, fail_count
        
        except Exception as e:
            logger.error(f"Failed to import users from file: {e}")
            return success_count, fail_count
    
    def export_users_to_file(self, filepath: str, export_format: str = "json") -> bool:
        """
        Export users to a file.
        
        Args:
            filepath (str): The path to the file.
            export_format (str, optional): The export format ("json" or "txt"). Defaults to "json".
            
        Returns:
            bool: True if the export was successful, False otherwise.
        """
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            if export_format.lower() == "json":
                # Export as JSON
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(self.users, f, indent=4)
            else:
                # Export as plain text, one user ID per line
                with open(filepath, 'w', encoding='utf-8') as f:
                    for user in self.users:
                        f.write(f"{user['user_id']}\n")
            
            logger.info(f"Exported {len(self.users)} users to {filepath}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to export users to file: {e}")
            return False