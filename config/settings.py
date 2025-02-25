"""
Configuration management for Discord Mass DM Tool.

This module provides functions for loading, saving, and accessing configuration settings.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("discord_dm_tool")

# Default configuration path
DEFAULT_CONFIG_FILE = Path(__file__).parent / "default_config.json"
USER_CONFIG_FILE = Path.home() / ".discord_dm_tool" / "config.json"


def load_default_config() -> Dict[str, Any]:
    """
    Load the default configuration from the default_config.json file.
    
    Returns:
        Dict[str, Any]: The default configuration as a dictionary.
    """
    try:
        with open(DEFAULT_CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load default configuration: {e}")
        # Return a minimal default configuration
        return {
            "rate_limits": {
                "messages_per_minute": 5,
                "friend_requests_per_minute": 2,
                "cooldown_period": 300  # seconds
            },
            "message_templates": [],
            "tokens": [],
            "users": [],
            "logging": {
                "level": "INFO",
                "file_enabled": True,
                "file_path": "logs/discord_dm_tool.log"
            }
        }


def load_user_config() -> Optional[Dict[str, Any]]:
    """
    Load the user configuration from the user's config file.
    
    Returns:
        Optional[Dict[str, Any]]: The user configuration as a dictionary, or None if not found.
    """
    if not USER_CONFIG_FILE.exists():
        logger.info("User configuration file not found")
        return None
    
    try:
        with open(USER_CONFIG_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load user configuration: {e}")
        return None


def merge_configs(default_config: Dict[str, Any], user_config: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge the default and user configurations, with user settings taking precedence.
    
    Args:
        default_config (Dict[str, Any]): The default configuration.
        user_config (Optional[Dict[str, Any]]): The user configuration, or None.
        
    Returns:
        Dict[str, Any]: The merged configuration.
    """
    if user_config is None:
        return default_config.copy()
    
    merged_config = default_config.copy()
    
    # Recursive function to merge nested dictionaries
    def merge_dicts(d1, d2):
        for k, v in d2.items():
            if k in d1 and isinstance(d1[k], dict) and isinstance(v, dict):
                merge_dicts(d1[k], v)
            else:
                d1[k] = v
    
    merge_dicts(merged_config, user_config)
    return merged_config


def load_config() -> Dict[str, Any]:
    """
    Load the configuration, merging default and user settings.
    
    Returns:
        Dict[str, Any]: The loaded configuration.
    """
    default_config = load_default_config()
    user_config = load_user_config()
    
    config = merge_configs(default_config, user_config)
    return config


def save_config(config: Dict[str, Any]) -> None:
    """
    Save the configuration to the user's config file.
    
    Args:
        config (Dict[str, Any]): The configuration to save.
    """
    # Create the directory if it doesn't exist
    USER_CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(USER_CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
        logger.info(f"Configuration saved to {USER_CONFIG_FILE}")
    except Exception as e:
        logger.error(f"Failed to save configuration: {e}")
        raise


def update_config_value(config: Dict[str, Any], key_path: str, value: Any) -> Dict[str, Any]:
    """
    Update a specific configuration value using a dot-notation key path.
    
    Args:
        config (Dict[str, Any]): The configuration dictionary.
        key_path (str): The key path in dot notation (e.g., 'rate_limits.messages_per_minute').
        value (Any): The new value to set.
        
    Returns:
        Dict[str, Any]: The updated configuration.
    """
    keys = key_path.split('.')
    
    # Navigate to the nested dictionary
    current = config
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    # Set the value
    current[keys[-1]] = value
    
    return config


def get_config_value(config: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """
    Get a specific configuration value using a dot-notation key path.
    
    Args:
        config (Dict[str, Any]): The configuration dictionary.
        key_path (str): The key path in dot notation (e.g., 'rate_limits.messages_per_minute').
        default (Any, optional): The default value to return if the key doesn't exist.
        
    Returns:
        Any: The configuration value, or the default if not found.
    """
    keys = key_path.split('.')
    
    # Navigate to the nested dictionary
    current = config
    for key in keys:
        if key not in current:
            return default
        current = current[key]
    
    return current