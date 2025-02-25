"""
Logging utilities for Discord Mass DM Tool.

This module provides functions for setting up and configuring logging.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import colorama
from colorama import Fore, Style

# Initialize colorama for cross-platform colored terminal output
colorama.init()


class ColoredFormatter(logging.Formatter):
    """Custom formatter to add colors to log messages based on their level."""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT
    }
    
    def format(self, record):
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{Style.RESET_ALL}"
            record.msg = f"{self.COLORS[levelname]}{record.msg}{Style.RESET_ALL}"
        return super().format(record)


def setup_logger(config: Optional[Dict[str, Any]] = None) -> logging.Logger:
    """
    Set up the logger for the application.
    
    Args:
        config (Optional[Dict[str, Any]]): Configuration dictionary containing logging settings.
        
    Returns:
        logging.Logger: The configured logger instance.
    """
    # Default logging settings if config is not provided
    log_level = "INFO"
    file_enabled = True
    file_path = "logs/discord_dm_tool.log"
    console_enabled = True
    
    # Override defaults with config if provided
    if config and "logging" in config:
        log_level = config["logging"].get("level", log_level)
        file_enabled = config["logging"].get("file_enabled", file_enabled)
        file_path = config["logging"].get("file_path", file_path)
        console_enabled = config["logging"].get("console_enabled", console_enabled)
    
    # Create logger
    logger = logging.getLogger("discord_dm_tool")
    logger.setLevel(getattr(logging, log_level))
    
    # Remove existing handlers to avoid duplicates when function is called multiple times
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create formatters
    file_formatter = logging.Formatter(
        '[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_formatter = ColoredFormatter(
        '[%(asctime)s] [%(levelname)s] - %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Add file handler if enabled
    if file_enabled:
        # Create the log directory if it doesn't exist
        log_dir = os.path.dirname(file_path)
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(file_path, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    # Add console handler if enabled
    if console_enabled:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    return logger


def get_logger() -> logging.Logger:
    """
    Get the application logger instance.
    
    Returns:
        logging.Logger: The logger instance.
    """
    return logging.getLogger("discord_dm_tool")