"""
Helper functions for Discord Mass DM Tool.

This module provides utility functions used throughout the application.
"""

import os
import re
import json
import time
import random
import string
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta

import requests
from tqdm import tqdm


def validate_token(token: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Validate a Discord token by making an API request.
    
    Args:
        token (str): The Discord token to validate.
        
    Returns:
        Tuple[bool, Optional[Dict[str, Any]]]: A tuple containing:
            - Boolean indicating if the token is valid
            - Dictionary with token information if valid, None otherwise
    """
    headers = {
        'Authorization': token,
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get('https://discord.com/api/v10/users/@me', headers=headers)
        
        if response.status_code == 200:
            user_data = response.json()
            return True, user_data
        else:
            return False, None
    except Exception:
        return False, None


def format_message(template: str, variables: Dict[str, str]) -> str:
    """
    Format a message template by replacing variables with their values.
    
    Args:
        template (str): The message template with placeholders like {variable_name}.
        variables (Dict[str, str]): A dictionary of variable names and their values.
        
    Returns:
        str: The formatted message.
    """
    formatted_message = template
    
    for key, value in variables.items():
        placeholder = f"{{{key}}}"
        formatted_message = formatted_message.replace(placeholder, value)
    
    return formatted_message


def extract_template_variables(template: str) -> List[str]:
    """
    Extract variable names from a message template.
    
    Args:
        template (str): The message template with placeholders like {variable_name}.
        
    Returns:
        List[str]: A list of variable names found in the template.
    """
    # Use regex to find all placeholders in the format {variable_name}
    pattern = r'\{([a-zA-Z0-9_]+)\}'
    matches = re.findall(pattern, template)
    
    # Return unique variable names
    return list(set(matches))


def apply_rate_limit(rate_per_minute: int, jitter_percent: int = 0) -> None:
    """
    Apply rate limiting by calculating and sleeping for the appropriate amount of time.
    
    Args:
        rate_per_minute (int): Maximum number of operations per minute.
        jitter_percent (int, optional): Percentage of randomness to add to the delay. Defaults to 0.
    """
    # Calculate base delay in seconds
    delay = 60 / rate_per_minute
    
    # Apply jitter if specified
    if jitter_percent > 0:
        jitter_factor = 1 + (random.uniform(-jitter_percent, jitter_percent) / 100)
        delay *= jitter_factor
    
    # Sleep for the calculated delay
    time.sleep(delay)


def generate_random_id(length: int = 10) -> str:
    """
    Generate a random alphanumeric ID.
    
    Args:
        length (int, optional): The length of the ID. Defaults to 10.
        
    Returns:
        str: A random alphanumeric ID.
    """
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))


def save_json_file(data: Any, filepath: str) -> None:
    """
    Save data to a JSON file.
    
    Args:
        data (Any): The data to save.
        filepath (str): The path to the file.
    """
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


def load_json_file(filepath: str, default: Any = None) -> Any:
    """
    Load data from a JSON file.
    
    Args:
        filepath (str): The path to the file.
        default (Any, optional): The default value to return if the file doesn't exist. Defaults to None.
        
    Returns:
        Any: The loaded data, or the default value if the file doesn't exist.
    """
    if not os.path.exists(filepath):
        return default
    
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_progress_bar(total: int, desc: str, unit: str = "items") -> tqdm:
    """
    Create a progress bar for tracking operations.
    
    Args:
        total (int): The total number of items to process.
        desc (str): The description of the progress bar.
        unit (str, optional): The unit of items. Defaults to "items".
        
    Returns:
        tqdm: A progress bar instance.
    """
    return tqdm(
        total=total,
        desc=desc,
        unit=unit,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    )


def format_timestamp(timestamp: Optional[float] = None) -> str:
    """
    Format a timestamp as a human-readable string.
    
    Args:
        timestamp (Optional[float], optional): The timestamp to format. Defaults to current time.
        
    Returns:
        str: The formatted timestamp.
    """
    if timestamp is None:
        timestamp = time.time()
    
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def parse_timestamp(timestamp_str: str) -> float:
    """
    Parse a timestamp string into a Unix timestamp.
    
    Args:
        timestamp_str (str): The timestamp string in format "YYYY-MM-DD HH:MM:SS".
        
    Returns:
        float: The Unix timestamp.
    """
    dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
    return dt.timestamp()


def is_snowflake(id_str: str) -> bool:
    """
    Check if a string is a valid Discord snowflake ID.
    
    Args:
        id_str (str): The string to check.
        
    Returns:
        bool: True if the string is a valid snowflake ID, False otherwise.
    """
    # Discord snowflakes are numeric and typically 17-19 digits
    return id_str.isdigit() and 17 <= len(id_str) <= 19


def calculate_cooldown_end(cooldown_seconds: int) -> str:
    """
    Calculate and format the end time of a cooldown period.
    
    Args:
        cooldown_seconds (int): The cooldown period in seconds.
        
    Returns:
        str: The formatted end time of the cooldown.
    """
    end_time = datetime.now() + timedelta(seconds=cooldown_seconds)
    return end_time.strftime("%H:%M:%S")