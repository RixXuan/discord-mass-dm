#!/usr/bin/env python3
"""
Discord Mass DM Tool - Main Entry Point

This script serves as the entry point for the Discord Mass DM tool.
It initializes the configuration, sets up logging, and starts the CLI interface.
"""

import os
import sys
import logging
from pathlib import Path

# Add the project root to the Python path
project_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_di)

from config.settings import load_config, save_config
from utils.logger import setup_logger
from ui.cli import CommandLineInterface


def main():
    """Main entry point for the Discord Mass DM tool."""
    # Setup logging
    setup_logger()
    logger = logging.getLogger("discord_dm_tool")
    logger.info("Starting Discord Mass DM Tool")

    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Initialize CLI
    try:
        cli = CommandLineInterface(config)
        cli.start()
    except KeyboardInterrupt:
        logger.info("Tool was terminated by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        import traceback
        logger.debug(traceback.format_exc())
    finally:
        try:
            save_config(config)
            logger.info("Configuration saved successfully")
        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")
        
        logger.info("Discord Mass DM Tool terminated")


if __name__ == "__main__":
    main()