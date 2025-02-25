"""
Command-line interface for Discord Mass DM Tool.

This module provides the main command-line interface for interacting with the tool.
"""

import os
import sys
import logging
import asyncio
import time
from typing import Dict, List, Any, Optional, Tuple, Callable
from tabulate import tabulate
import pyfiglet
from colorama import Fore, Style, init as colorama_init

from core.token_manager import TokenManager
from core.user_manager import UserManager
from core.message_manager import MessageManager
from core.stats_manager import StatsManager
from services.scraper import MemberScraper
from services.dm_sender import DMSender
from services.friend_manager import FriendManager
from utils.helpers import format_timestamp, calculate_cooldown_end
from ui.prompts import (
    print_color, print_header, print_subheader, print_success, print_info,
    print_warning, print_error, clear_screen, prompt_input, prompt_yes_no,
    prompt_options, prompt_multi_select, prompt_filepath, prompt_token,
    prompt_user_id, prompt_server_id, prompt_message_template, prompt_integer,
    prompt_float, pause
)

# Initialize colorama
colorama_init(autoreset=True)

logger = logging.getLogger("discord_dm_tool")


class CommandLineInterface:
    """
    Command-line interface for the Discord Mass DM Tool.
    
    Provides menus and commands for interacting with the tool.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the CommandLineInterface.
        
        Args:
            config (Dict[str, Any]): The application configuration.
        """
        self.config = config
        
        # Initialize managers
        self.token_manager = TokenManager(config)
        self.user_manager = UserManager(config)
        self.message_manager = MessageManager(config)
        self.stats_manager = StatsManager(config)
        
        # Initialize services
        self.scraper = MemberScraper(config, self.token_manager, self.user_manager)
        self.dm_sender = DMSender(config, self.token_manager, self.user_manager, 
                              self.message_manager, self.stats_manager)
        self.friend_manager = FriendManager(config, self.token_manager, 
                                       self.user_manager, self.stats_manager)
        
        # Set up event loop
        self.loop = asyncio.get_event_loop()
        
        logger.debug("CommandLineInterface initialized")
    
    def start(self) -> None:
        """Start the command-line interface."""
        self._show_welcome()
        
        while True:
            try:
                self._show_main_menu()
            except KeyboardInterrupt:
                print_warning("\nOperation cancelled by user")
                if prompt_yes_no("Do you want to exit?"):
                    break
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
                print_error(f"An unexpected error occurred: {e}")
                pause()
    
    def _show_welcome(self) -> None:
        """Show the welcome screen."""
        clear_screen()
        
        # Print logo
        logo = pyfiglet.figlet_format("Discord Mass DM Tool", font="slant")
        print_color(logo, Fore.CYAN)
        
        print_color("Welcome to Discord Mass DM Tool", Fore.GREEN, bold=True)
        print_color("A modular tool for sending direct messages to multiple Discord users\n", Fore.GREEN)
        
        print_info(f"Tokens loaded: {self.token_manager.get_token_count()}")
        print_info(f"Users loaded: {self.user_manager.get_user_count()}")
        print_info(f"Message templates: {self.message_manager.get_template_count()}\n")
        
        pause()
    
    def _show_main_menu(self) -> None:
        """Show the main menu."""
        clear_screen()
        
        print_header("Main Menu")
        
        menu_options = {
            "1": "Token Management",
            "2": "User Management",
            "3": "Message Templates",
            "4": "Server Scraper",
            "5": "Send DMs",
            "6": "Friend Requests",
            "7": "Statistics",
            "8": "Settings",
            "9": "Exit"
        }
        
        for key, value in menu_options.items():
            print_color(f"  {key}. {value}")
        
        print()
        choice = prompt_input("Enter your choice")
        
        if choice == "1":
            self._show_token_menu()
        elif choice == "2":
            self._show_user_menu()
        elif choice == "3":
            self._show_template_menu()
        elif choice == "4":
            self._show_scraper_menu()
        elif choice == "5":
            self._show_dm_menu()
        elif choice == "6":
            self._show_friend_menu()
        elif choice == "7":
            self._show_stats_menu()
        elif choice == "8":
            self._show_settings_menu()
        elif choice == "9":
            if prompt_yes_no("Are you sure you want to exit?"):
                sys.exit(0)
        else:
            print_error("Invalid choice")
            pause()
    
    def _show_token_menu(self) -> None:
        """Show the token management menu."""
        while True:
            clear_screen()
            
            print_header("Token Management")
            
            tokens = self.token_manager.get_all_tokens()
            
            if tokens:
                # Prepare token data for display
                token_data = []
                for token in tokens:
                    # Truncate token for display
                    display_token = token["token"][:10] + "..." + token["token"][-5:]
                    
                    # Get metadata
                    metadata = token.get("metadata", {})
                    username = metadata.get("username", "N/A")
                    user_id = metadata.get("user_id", "N/A")
                    
                    token_data.append([
                        token["id"],
                        display_token,
                        token["alias"],
                        username,
                        user_id,
                        format_timestamp(token["added_at"])
                    ])
                
                # Display tokens table
                print(tabulate(
                    token_data,
                    headers=["ID", "Token", "Alias", "Username", "User ID", "Added At"],
                    tablefmt="grid"
                ))
                print()
            else:
                print_warning("No tokens found\n")
            
            print_subheader("Options")
            menu_options = {
                "1": "Add Token",
                "2": "Remove Token",
                "3": "Validate Tokens",
                "4": "Import Tokens from File",
                "5": "Back to Main Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._add_token()
            elif choice == "2":
                self._remove_token()
            elif choice == "3":
                self._validate_tokens()
            elif choice == "4":
                self._import_tokens()
            elif choice == "5":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _add_token(self) -> None:
        """Add a new token."""
        print_subheader("Add Token")
        
        token = prompt_token("Enter Discord token")
        if not token:
            return
        
        alias = prompt_input("Enter alias for this token (optional)")
        
        print_info("Validating token...")
        success, error = self.token_manager.add_token(token, alias)
        
        if success:
            print_success("Token added successfully")
        else:
            print_error(f"Failed to add token: {error}")
        
        pause()
    
    def _remove_token(self) -> None:
        """Remove a token."""
        print_subheader("Remove Token")
        
        tokens = self.token_manager.get_all_tokens()
        if not tokens:
            print_warning("No tokens to remove")
            pause()
            return
        
        # Create options for selection
        options = {}
        for token in tokens:
            display_token = token["token"][:10] + "..." + token["token"][-5:]
            options[token["id"]] = f"{token['alias']} ({display_token})"
        
        token_id = prompt_options("Select token to remove", options)
        if not token_id:
            return
        
        if prompt_yes_no(f"Are you sure you want to remove token '{options[token_id]}'?"):
            success = self.token_manager.remove_token(token_id)
            
            if success:
                print_success("Token removed successfully")
            else:
                print_error("Failed to remove token")
        
        pause()
    
    def _validate_tokens(self) -> None:
        """Validate all tokens."""
        print_subheader("Validate Tokens")
        
        tokens = self.token_manager.get_all_tokens()
        if not tokens:
            print_warning("No tokens to validate")
            pause()
            return
        
        print_info(f"Validating {len(tokens)} tokens...")
        results = self.token_manager.validate_all_tokens()
        
        valid_count = sum(1 for valid in results.values() if valid)
        invalid_count = len(results) - valid_count
        
        print_success(f"Validation complete: {valid_count} valid, {invalid_count} invalid")
        
        if invalid_count > 0:
            print_warning("Invalid tokens:")
            for token_id, valid in results.items():
                if not valid:
                    token = self.token_manager.get_token(token_id)
                    if token:
                        print_color(f"  - {token['alias']}", Fore.RED)
            
            if prompt_yes_no("Do you want to remove invalid tokens?"):
                for token_id, valid in results.items():
                    if not valid:
                        self.token_manager.remove_token(token_id)
                
                print_success("Invalid tokens removed")
        
        pause()
    
    def _import_tokens(self) -> None:
        """Import tokens from a file."""
        print_subheader("Import Tokens from File")
        
        print_info("The file should contain one token per line")
        filepath = prompt_filepath("Enter filepath")
        
        if not filepath:
            return
        
        print_info("Importing tokens...")
        success_count, fail_count = self.token_manager.import_tokens_from_file(filepath)
        
        print_success(f"Import complete: {success_count} imported, {fail_count} failed")
        pause()
    
    def _show_user_menu(self) -> None:
        """Show the user management menu."""
        while True:
            clear_screen()
            
            print_header("User Management")
            
            users = self.user_manager.get_all_users()
            
            if users:
                # Display user count
                print_info(f"Total users: {len(users)}")
                print_info(f"Unmessaged users: {len(self.user_manager.get_unmessaged_users())}\n")
                
                # Display users table (limited to 10 for readability)
                display_limit = 10
                user_data = []
                for i, user in enumerate(users[:display_limit]):
                    user_data.append([
                        user["id"],
                        user["user_id"],
                        user["username"],
                        format_timestamp(user["added_at"])
                    ])
                
                print(tabulate(
                    user_data,
                    headers=["ID", "User ID", "Username", "Added At"],
                    tablefmt="grid"
                ))
                
                if len(users) > display_limit:
                    print_info(f"... and {len(users) - display_limit} more users\n")
                print()
            else:
                print_warning("No users found\n")
            
            # User groups
            groups = self.user_manager.get_user_groups()
            if groups:
                print_subheader("User Groups")
                group_data = []
                for group_name, user_ids in groups.items():
                    group_data.append([group_name, len(user_ids)])
                
                print(tabulate(
                    group_data,
                    headers=["Group Name", "User Count"],
                    tablefmt="simple"
                ))
                print()
            
            print_subheader("Options")
            menu_options = {
                "1": "Add User",
                "2": "Remove User",
                "3": "Import Users from File",
                "4": "Export Users to File",
                "5": "Manage User Groups",
                "6": "Reset Message Status",
                "7": "Back to Main Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._add_user()
            elif choice == "2":
                self._remove_user()
            elif choice == "3":
                self._import_users()
            elif choice == "4":
                self._export_users()
            elif choice == "5":
                self._manage_user_groups()
            elif choice == "6":
                self._reset_message_status()
            elif choice == "7":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _add_user(self) -> None:
        """Add a new user."""
        print_subheader("Add User")
        
        user_id = prompt_user_id("Enter Discord user ID")
        if not user_id:
            return
        
        username = prompt_input("Enter username (optional)")
        
        success, error = self.user_manager.add_user(user_id, username)
        
        if success:
            print_success("User added successfully")
        else:
            print_error(f"Failed to add user: {error}")
        
        pause()
    
    def _remove_user(self) -> None:
        """Remove a user."""
        print_subheader("Remove User")
        
        users = self.user_manager.get_all_users()
        if not users:
            print_warning("No users to remove")
            pause()
            return
        
        # Create options for selection
        options = {}
        for user in users:
            options[user["id"]] = f"{user['username']} ({user['user_id']})"
        
        user_id = prompt_options("Select user to remove", options)
        if not user_id:
            return
        
        if prompt_yes_no(f"Are you sure you want to remove user '{options[user_id]}'?"):
            success = self.user_manager.remove_user(user_id)
            
            if success:
                print_success("User removed successfully")
            else:
                print_error("Failed to remove user")
        
        pause()
    
    def _import_users(self) -> None:
        """Import users from a file."""
        print_subheader("Import Users from File")
        
        print_info("The file should contain one user ID per line or be in JSON format")
        filepath = prompt_filepath("Enter filepath")
        
        if not filepath:
            return
        
        print_info("Importing users...")
        success_count, fail_count = self.user_manager.import_users_from_file(filepath)
        
        print_success(f"Import complete: {success_count} imported, {fail_count} failed")
        pause()
    
    def _export_users(self) -> None:
        """Export users to a file."""
        print_subheader("Export Users to File")
        
        users = self.user_manager.get_all_users()
        if not users:
            print_warning("No users to export")
            pause()
            return
        
        filepath = prompt_input("Enter filepath")
        if not filepath:
            return
        
        format_options = {
            "json": "JSON format (detailed)",
            "txt": "Text format (IDs only)"
        }
        
        format_type = prompt_options("Select export format", format_options)
        if not format_type:
            return
        
        print_info("Exporting users...")
        success = self.user_manager.export_users_to_file(filepath, format_type)
        
        if success:
            print_success(f"Exported {len(users)} users to {filepath}")
        else:
            print_error("Failed to export users")
        
        pause()
    
    def _manage_user_groups(self) -> None:
        """Manage user groups."""
        while True:
            clear_screen()
            
            print_header("Manage User Groups")
            
            groups = self.user_manager.get_user_groups()
            
            if groups:
                group_data = []
                for group_name, user_ids in groups.items():
                    group_data.append([group_name, len(user_ids)])
                
                print(tabulate(
                    group_data,
                    headers=["Group Name", "User Count"],
                    tablefmt="grid"
                ))
                print()
            else:
                print_warning("No user groups found\n")
            
            print_subheader("Options")
            menu_options = {
                "1": "Create Group",
                "2": "Delete Group",
                "3": "Add Users to Group",
                "4": "Remove Users from Group",
                "5": "Back to User Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._create_user_group()
            elif choice == "2":
                self._delete_user_group()
            elif choice == "3":
                self._add_users_to_group()
            elif choice == "4":
                self._remove_users_from_group()
            elif choice == "5":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _create_user_group(self) -> None:
        """Create a new user group."""
        print_subheader("Create User Group")
        
        group_name = prompt_input("Enter group name")
        if not group_name:
            return
        
        success = self.user_manager.add_user_group(group_name)
        
        if success:
            print_success(f"Group '{group_name}' created successfully")
        else:
            print_error(f"Group '{group_name}' already exists")
        
        pause()
    
    def _delete_user_group(self) -> None:
        """Delete a user group."""
        print_subheader("Delete User Group")
        
        groups = self.user_manager.get_user_groups()
        if not groups:
            print_warning("No user groups to delete")
            pause()
            return
        
        options = {name: f"{name} ({len(users)} users)" for name, users in groups.items()}
        
        group_name = prompt_options("Select group to delete", options)
        if not group_name:
            return
        
        if prompt_yes_no(f"Are you sure you want to delete group '{group_name}'?"):
            success = self.user_manager.remove_user_group(group_name)
            
            if success:
                print_success(f"Group '{group_name}' deleted successfully")
            else:
                print_error(f"Failed to delete group '{group_name}'")
        
        pause()
    
    def _add_users_to_group(self) -> None:
        """Add users to a group."""
        print_subheader("Add Users to Group")
        
        groups = self.user_manager.get_user_groups()
        if not groups:
            print_warning("No user groups available")
            pause()
            return
        
        group_options = {name: f"{name} ({len(users)} users)" for name, users in groups.items()}
        
        group_name = prompt_options("Select group", group_options)
        if not group_name:
            return
        
        users = self.user_manager.get_all_users()
        if not users:
            print_warning("No users available")
            pause()
            return
        
        # Get current users in the group
        group_user_ids = groups[group_name]
        
        # Create options for users not already in the group
        user_options = {}
        for user in users:
            if user["user_id"] not in group_user_ids:
                user_options[user["user_id"]] = f"{user['username']} ({user['user_id']})"
        
        if not user_options:
            print_warning("All users are already in the group")
            pause()
            return
        
        selected_user_ids = prompt_multi_select("Select users to add", user_options)
        if not selected_user_ids:
            return
        
        added_count = 0
        for user_id in selected_user_ids:
            success = self.user_manager.add_user_to_group(group_name, user_id)
            if success:
                added_count += 1
        
        print_success(f"Added {added_count} users to group '{group_name}'")
        pause()
    
    def _remove_users_from_group(self) -> None:
        """Remove users from a group."""
        print_subheader("Remove Users from Group")
        
        groups = self.user_manager.get_user_groups()
        if not groups:
            print_warning("No user groups available")
            pause()
            return
        
        group_options = {name: f"{name} ({len(users)} users)" for name, users in groups.items()}
        
        group_name = prompt_options("Select group", group_options)
        if not group_name:
            return
        
        # Get current users in the group
        group_user_ids = groups[group_name]
        if not group_user_ids:
            print_warning("No users in the group")
            pause()
            return
        
        # Create options for users in the group
        user_options = {}
        for user_id in group_user_ids:
            user = self.user_manager.get_user_by_discord_id(user_id)
            if user:
                user_options[user_id] = f"{user['username']} ({user_id})"
            else:
                user_options[user_id] = f"Unknown User ({user_id})"
        
        selected_user_ids = prompt_multi_select("Select users to remove", user_options)
        if not selected_user_ids:
            return
        
        removed_count = 0
        for user_id in selected_user_ids:
            success = self.user_manager.remove_user_from_group(group_name, user_id)
            if success:
                removed_count += 1
        
        print_success(f"Removed {removed_count} users from group '{group_name}'")
        pause()
    
    def _reset_message_status(self) -> None:
        """Reset message status for users."""
        print_subheader("Reset Message Status")
        
        options = {
            "all": "Reset for all users",
            "specific": "Reset for specific user"
        }
        
        choice = prompt_options("Select reset option", options)
        if not choice:
            return
        
        if choice == "all":
            if prompt_yes_no("Are you sure you want to reset message status for ALL users?"):
                self.user_manager.reset_message_status()
                print_success("Message status reset for all users")
        
        elif choice == "specific":
            user_id = prompt_user_id("Enter Discord user ID")
            if not user_id:
                return
            
            self.user_manager.reset_message_status(user_id)
            print_success(f"Message status reset for user '{user_id}'")
        
        pause()
    
    def _show_template_menu(self) -> None:
        """Show the message template menu."""
        while True:
            clear_screen()
            
            print_header("Message Templates")
            
            templates = self.message_manager.get_all_templates()
            
            if templates:
                # Display templates table
                template_data = []
                for template in templates:
                    # Truncate content for display
                    content = template["content"]
                    if len(content) > 50:
                        content = content[:47] + "..."
                    
                    template_data.append([
                        template["id"],
                        template["name"],
                        content,
                        ", ".join(template["variables"]),
                        format_timestamp(template["created_at"])
                    ])
                
                print(tabulate(
                    template_data,
                    headers=["ID", "Name", "Content", "Variables", "Created At"],
                    tablefmt="grid"
                ))
                print()
            else:
                print_warning("No templates found\n")
            
            print_subheader("Options")
            menu_options = {
                "1": "Create Template",
                "2": "Edit Template",
                "3": "Delete Template",
                "4": "View Template",
                "5": "Import Templates from File",
                "6": "Export Templates to File",
                "7": "Back to Main Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._create_template()
            elif choice == "2":
                self._edit_template()
            elif choice == "3":
                self._delete_template()
            elif choice == "4":
                self._view_template()
            elif choice == "5":
                self._import_templates()
            elif choice == "6":
                self._export_templates()
            elif choice == "7":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _create_template(self) -> None:
        """Create a new message template."""
        print_subheader("Create Template")
        
        name = prompt_input("Enter template name")
        if not name:
            return
        
        print_info("Enter template content (use variables like {username})")
        content = prompt_message_template()
        if not content:
            return
        
        template_id = self.message_manager.add_template(name, content)
        print_success(f"Template '{name}' created successfully")
        
        # Show detected variables
        template = self.message_manager.get_template(template_id)
        if template and template["variables"]:
            print_info(f"Detected variables: {', '.join(template['variables'])}")
        
        pause()
    
    def _edit_template(self) -> None:
        """Edit a message template."""
        print_subheader("Edit Template")
        
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_warning("No templates to edit")
            pause()
            return
        
        # Create options for selection
        options = {}
        for template in templates:
            options[template["id"]] = template["name"]
        
        template_id = prompt_options("Select template to edit", options)
        if not template_id:
            return
        
        template = self.message_manager.get_template(template_id)
        if not template:
            print_error("Template not found")
            pause()
            return
        
        print_info(f"Current name: {template['name']}")
        new_name = prompt_input("Enter new name (or leave empty to keep current)")
        
        print_info("Current content:")
        print_color(template["content"], Fore.CYAN)
        print()
        
        print_info("Enter new content (or leave empty to keep current)")
        new_content = prompt_message_template()
        
        if not new_name and not new_content:
            print_warning("No changes made")
            pause()
            return
        
        success = self.message_manager.update_template(
            template_id,
            name=new_name if new_name else None,
            content=new_content if new_content else None
        )
        
        if success:
            print_success("Template updated successfully")
            
            # Show updated template
            updated_template = self.message_manager.get_template(template_id)
            if updated_template and updated_template["variables"]:
                print_info(f"Template variables: {', '.join(updated_template['variables'])}")
        else:
            print_error("Failed to update template")
        
        pause()
    
    def _delete_template(self) -> None:
        """Delete a message template."""
        print_subheader("Delete Template")
        
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_warning("No templates to delete")
            pause()
            return
        
        # Create options for selection
        options = {}
        for template in templates:
            options[template["id"]] = template["name"]
        
        template_id = prompt_options("Select template to delete", options)
        if not template_id:
            return
        
        if prompt_yes_no(f"Are you sure you want to delete template '{options[template_id]}'?"):
            success = self.message_manager.remove_template(template_id)
            
            if success:
                print_success("Template deleted successfully")
            else:
                print_error("Failed to delete template")
        
        pause()
    
    def _view_template(self) -> None:
        """View a message template."""
        print_subheader("View Template")
        
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_warning("No templates to view")
            pause()
            return
        
        # Create options for selection
        options = {}
        for template in templates:
            options[template["id"]] = template["name"]
        
        template_id = prompt_options("Select template to view", options)
        if not template_id:
            return
        
        template = self.message_manager.get_template(template_id)
        if not template:
            print_error("Template not found")
            pause()
            return
        
        print_subheader(f"Template: {template['name']}")
        print_info(f"ID: {template['id']}")
        print_info(f"Created: {format_timestamp(template['created_at'])}")
        if "updated_at" in template:
            print_info(f"Updated: {format_timestamp(template['updated_at'])}")
        print_info(f"Variables: {', '.join(template['variables'])}")
        print()
        print_color("Content:", Fore.YELLOW)
        print_color(template["content"], Fore.CYAN)
        
        pause()
    
    def _import_templates(self) -> None:
        """Import templates from a file."""
        print_subheader("Import Templates from File")
        
        print_info("The file should be in JSON format")
        filepath = prompt_filepath("Enter filepath")
        
        if not filepath:
            return
        
        print_info("Importing templates...")
        success_count, fail_count = self.message_manager.import_templates_from_file(filepath)
        
        print_success(f"Import complete: {success_count} imported, {fail_count} failed")
        pause()
    
    def _export_templates(self) -> None:
        """Export templates to a file."""
        print_subheader("Export Templates to File")
        
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_warning("No templates to export")
            pause()
            return
        
        filepath = prompt_input("Enter filepath")
        if not filepath:
            return
        
        print_info("Exporting templates...")
        success = self.message_manager.export_templates_to_file(filepath)
        
        if success:
            print_success(f"Exported {len(templates)} templates to {filepath}")
        else:
            print_error("Failed to export templates")
        
        pause()
    
    def _show_scraper_menu(self) -> None:
        """Show the server scraper menu."""
        while True:
            clear_screen()
            
            print_header("Server Scraper")
            
            # Show scraping history
            history = self.scraper.get_scrape_history()
            if history:
                print_subheader("Scraping History")
                
                history_data = []
                for server_id, data in history.items():
                    history_data.append([
                        server_id,
                        data["formatted_time"]
                    ])
                
                print(tabulate(
                    history_data,
                    headers=["Server ID", "Last Scraped"],
                    tablefmt="simple"
                ))
                print()
            
            print_subheader("Options")
            menu_options = {
                "1": "Scrape Server Members",
                "2": "Clear Scrape History",
                "3": "Back to Main Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._scrape_server()
            elif choice == "2":
                self._clear_scrape_history()
            elif choice == "3":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _scrape_server(self) -> None:
        """Scrape members from a server."""
        print_subheader("Scrape Server Members")
        
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available for scraping")
            print_info("Please add tokens first")
            pause()
            return
        
        server_id = prompt_server_id("Enter Discord server ID")
        if not server_id:
            return
        
        token_options = {
            "auto": "Auto-select token",
        }
        
        for token in self.token_manager.get_all_tokens():
            token_options[token["id"]] = f"{token['alias']} ({token['token'][:10]}...)"
        
        token_choice = prompt_options("Select token to use", token_options)
        if not token_choice:
            return
        
        # Get the token if not auto-select
        token = None
        if token_choice != "auto":
            token_data = self.token_manager.get_token(token_choice)
            if token_data:
                token = token_data["token"]
        
        print_info(f"Scraping members from server {server_id}...")
        print_info("This may take a while depending on the server size.")
        print_warning("Make sure the token has access to the server!")
        
        # Run the scraper
        try:
            members_scraped, new_members, status = self.loop.run_until_complete(
                self.scraper.scrape_server(server_id, token)
            )
            
            if status == "Success":
                print_success(f"Scraping complete: {members_scraped} members scraped, {new_members} new members added")
            else:
                print_error(f"Scraping failed: {status}")
        
        except Exception as e:
            logger.error(f"Error during scraping: {e}")
            print_error(f"Error during scraping: {e}")
        
        pause()
    
    def _clear_scrape_history(self) -> None:
        """Clear server scraping history."""
        print_subheader("Clear Scrape History")
        
        if prompt_yes_no("Are you sure you want to clear scraping history?"):
            self.scraper.clear_scrape_history()
            print_success("Scrape history cleared")
        
        pause()
    
    def _show_dm_menu(self) -> None:
        """Show the DM sending menu."""
        while True:
            clear_screen()
            
            print_header("Send DMs")
            
            # Check if DM operation is running
            if self.dm_sender.is_running():
                print_info("A DM operation is currently running")
                
                tasks = self.dm_sender.get_running_tasks()
                if tasks:
                    task = tasks[0]  # Get the first task
                    
                    print_info(f"Progress: {task['progress']}/{task['user_count']} users")
                    print_info(f"Success: {task['success_count']}, Fails: {task['fail_count']}, Rate Limits: {task['rate_limit_count']}")
                    print_info(f"Running for: {task['duration']:.2f} seconds")
                    print()
                
                print_subheader("Options")
                menu_options = {
                    "1": "Stop DM Operation",
                    "2": "Back to Main Menu"
                }
            else:
                # Show tokens and users count
                print_info(f"Available tokens: {self.token_manager.get_token_count()}")
                print_info(f"Available users: {self.user_manager.get_user_count()}")
                print_info(f"Unmessaged users: {len(self.user_manager.get_unmessaged_users())}")
                print_info(f"Available templates: {self.message_manager.get_template_count()}")
                print()
                
                print_subheader("Options")
                menu_options = {
                    "1": "Send DMs to All Unmessaged Users",
                    "2": "Send DMs to User Group",
                    "3": "Send DM to Specific User",
                    "4": "Back to Main Menu"
                }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if self.dm_sender.is_running():
                if choice == "1":
                    self._stop_dm_operation()
                elif choice == "2":
                    break
                else:
                    print_error("Invalid choice")
                    pause()
            else:
                if choice == "1":
                    self._send_dms_to_unmessaged()
                elif choice == "2":
                    self._send_dms_to_group()
                elif choice == "3":
                    self._send_dm_to_user()
                elif choice == "4":
                    break
                else:
                    print_error("Invalid choice")
                    pause()
    
    def _send_dms_to_unmessaged(self) -> None:
        """Send DMs to all unmessaged users."""
        print_subheader("Send DMs to All Unmessaged Users")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Get unmessaged users
        unmessaged_users = self.user_manager.get_unmessaged_users()
        if not unmessaged_users:
            print_warning("No unmessaged users found")
            pause()
            return
        
        # Select template
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_error("No message templates available")
            print_info("Please create a template first")
            pause()
            return
        
        template_options = {}
        for template in templates:
            template_options[template["id"]] = template["name"]
        
        template_id = prompt_options("Select message template", template_options)
        if not template_id:
            return
        
        # Get template variables
        template = self.message_manager.get_template(template_id)
        if not template:
            print_error("Template not found")
            pause()
            return
        
        variables = {}
        for var in template["variables"]:
            if var != "username":  # Username is handled automatically
                value = prompt_input(f"Enter value for {{{var}}}")
                variables[var] = value
        
        # Confirm sending
        user_count = len(unmessaged_users)
        
        print_info(f"Ready to send DMs to {user_count} users")
        print_info(f"Using template: {template['name']}")
        
        if not prompt_yes_no("Continue with sending?"):
            return
        
        # Start the DM operation
        user_ids = [user["user_id"] for user in unmessaged_users]
        
        # Progress callback
        def update_progress(current, total, success, fails):
            # Calculate percentage
            percent = int(current / total * 100) if total > 0 else 0
            
            # Create progress bar
            bar_width = 40
            filled_width = int(bar_width * percent / 100)
            bar = "█" * filled_width + "-" * (bar_width - filled_width)
            
            # Print progress
            print(f"\r[{bar}] {percent}% | {current}/{total} | ✓ {success} | ✗ {fails}", end="")
        
        print_info("Starting DM operation...")
        print()
        
        try:
            # Start the task
            task = self.dm_sender.start_bulk_dms(template_id, user_ids, variables, update_progress)
            
            if task:
                # Wait for the task to complete
                result = self.loop.run_until_complete(task)
                
                print("\n")  # Add newline after progress bar
                
                if result["status"] == "complete":
                    print_success(f"DM operation completed: {result['success_count']} successful, {result['fail_count']} failed")
                else:
                    print_error(f"DM operation failed: {result['message']}")
            else:
                print_error("Failed to start DM operation")
        
        except Exception as e:
            logger.error(f"Error during DM operation: {e}")
            print_error(f"Error during DM operation: {e}")
        
        pause()
    
    def _send_dms_to_group(self) -> None:
        """Send DMs to a user group."""
        print_subheader("Send DMs to User Group")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Get user groups
        groups = self.user_manager.get_user_groups()
        if not groups:
            print_warning("No user groups found")
            print_info("Please create a user group first")
            pause()
            return
        
        # Select group
        group_options = {name: f"{name} ({len(users)} users)" for name, users in groups.items()}
        
        group_name = prompt_options("Select user group", group_options)
        if not group_name:
            return
        
        # Get users in the group
        group_users = self.user_manager.get_users_in_group(group_name)
        if not group_users:
            print_warning(f"No users found in group '{group_name}'")
            pause()
            return
        
        # Select template
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_error("No message templates available")
            print_info("Please create a template first")
            pause()
            return
        
        template_options = {}
        for template in templates:
            template_options[template["id"]] = template["name"]
        
        template_id = prompt_options("Select message template", template_options)
        if not template_id:
            return
        
        # Get template variables
        template = self.message_manager.get_template(template_id)
        if not template:
            print_error("Template not found")
            pause()
            return
        
        variables = {}
        for var in template["variables"]:
            if var != "username":  # Username is handled automatically
                value = prompt_input(f"Enter value for {{{var}}}")
                variables[var] = value
        
        # Optional: filter for unmessaged only
        filter_option = prompt_yes_no("Send only to unmessaged users in the group?", default=True)
        
        user_ids = []
        if filter_option:
            unmessaged_users = self.user_manager.get_unmessaged_users()
            unmessaged_ids = [u["user_id"] for u in unmessaged_users]
            user_ids = [u["user_id"] for u in group_users if u["user_id"] in unmessaged_ids]
        else:
            user_ids = [u["user_id"] for u in group_users]
        
        if not user_ids:
            print_warning("No users to message after filtering")
            pause()
            return
        
        # Confirm sending
        user_count = len(user_ids)
        
        print_info(f"Ready to send DMs to {user_count} users in group '{group_name}'")
        print_info(f"Using template: {template['name']}")
        
        if not prompt_yes_no("Continue with sending?"):
            return
        
        # Progress callback
        def update_progress(current, total, success, fails):
            # Calculate percentage
            percent = int(current / total * 100) if total > 0 else 0
            
            # Create progress bar
            bar_width = 40
            filled_width = int(bar_width * percent / 100)
            bar = "█" * filled_width + "-" * (bar_width - filled_width)
            
            # Print progress
            print(f"\r[{bar}] {percent}% | {current}/{total} | ✓ {success} | ✗ {fails}", end="")
        
        print_info("Starting DM operation...")
        print()
        
        try:
            # Start the task
            task = self.dm_sender.start_bulk_dms(template_id, user_ids, variables, update_progress)
            
            if task:
                # Wait for the task to complete
                result = self.loop.run_until_complete(task)
                
                print("\n")  # Add newline after progress bar
                
                if result["status"] == "complete":
                    print_success(f"DM operation completed: {result['success_count']} successful, {result['fail_count']} failed")
                else:
                    print_error(f"DM operation failed: {result['message']}")
            else:
                print_error("Failed to start DM operation")
        
        except Exception as e:
            logger.error(f"Error during DM operation: {e}")
            print_error(f"Error during DM operation: {e}")
        
        pause()
    
    def _send_dm_to_user(self) -> None:
        """Send a DM to a specific user."""
        print_subheader("Send DM to Specific User")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Get user ID
        user_id_input = prompt_input("Enter Discord user ID (or search by username)")
        if not user_id_input:
            return
        
        # Check if input is a user ID or search term
        user = None
        if user_id_input.isdigit() and len(user_id_input) >= 17:
            # It's probably a user ID
            user = self.user_manager.get_user_by_discord_id(user_id_input)
        else:
            # Search by username
            users = self.user_manager.get_all_users()
            matching_users = [u for u in users if user_id_input.lower() in u["username"].lower()]
            
            if not matching_users:
                print_warning(f"No users found matching '{user_id_input}'")
                pause()
                return
            
            # If multiple matches, let user select
            if len(matching_users) > 1:
                user_options = {}
                for u in matching_users:
                    user_options[u["user_id"]] = f"{u['username']} ({u['user_id']})"
                
                selected_id = prompt_options("Select user", user_options)
                if not selected_id:
                    return
                
                user = self.user_manager.get_user_by_discord_id(selected_id)
            else:
                user = matching_users[0]
        
        if not user:
            # If still no user found, ask to add it
            if prompt_yes_no(f"User '{user_id_input}' not found. Add it?"):
                success, _ = self.user_manager.add_user(user_id_input)
                if success:
                    user = self.user_manager.get_user_by_discord_id(user_id_input)
                    print_success(f"User '{user_id_input}' added")
                else:
                    print_error(f"Failed to add user '{user_id_input}'")
                    pause()
                    return
            else:
                return
        
        # Select template
        templates = self.message_manager.get_all_templates()
        if not templates:
            print_error("No message templates available")
            print_info("Please create a template first")
            pause()
            return
        
        template_options = {}
        for template in templates:
            template_options[template["id"]] = template["name"]
        
        template_id = prompt_options("Select message template", template_options)
        if not template_id:
            return
        
        # Get template variables
        template = self.message_manager.get_template(template_id)
        if not template:
            print_error("Template not found")
            pause()
            return
        
        variables = {}
        for var in template["variables"]:
            if var == "username":
                variables[var] = user["username"]
            else:
                value = prompt_input(f"Enter value for {{{var}}}")
                variables[var] = value
        
        # Select token
        token_options = {
            "auto": "Auto-select token",
        }
        
        for token in self.token_manager.get_all_tokens():
            token_options[token["id"]] = f"{token['alias']} ({token['token'][:10]}...)"
        
        token_choice = prompt_options("Select token to use", token_options)
        if not token_choice:
            return
        
        # Get the token if not auto-select
        token = None
        if token_choice != "auto":
            token_data = self.token_manager.get_token(token_choice)
            if token_data:
                token = token_data["token"]
        else:
            token = self.token_manager.get_next_token()
            if not token:
                print_error("No available tokens")
                pause()
                return
        
        # Format message
        message = self.message_manager.format_template(template_id, variables)
        if not message:
            print_error("Failed to format message")
            pause()
            return
        
        # Confirm sending
        print_info(f"Ready to send DM to user '{user['username']}' ({user['user_id']})")
        print_info("Message preview:")
        print_color(message, Fore.CYAN)
        
        if not prompt_yes_no("Send this message?"):
            return
        
        # Send the message
        print_info(f"Sending DM to user '{user['username']}'...")
        
        try:
            success, status, metadata = self.loop.run_until_complete(
                self.dm_sender.send_dm(user["user_id"], message, token)
            )
            
            if success:
                print_success("Message sent successfully")
                
                # Mark as messaged
                self.user_manager.mark_user_as_messaged(user["user_id"], "sent", metadata)
                
                # Track statistics
                self.stats_manager.track_message_sent(user["user_id"], token, "success", metadata)
            else:
                print_error(f"Failed to send message: {status}")
                
                # Track statistics
                self.stats_manager.track_message_sent(user["user_id"], token, "failed", metadata)
        
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            print_error(f"Error sending message: {e}")
        
        pause()
    
    def _stop_dm_operation(self) -> None:
        """Stop the running DM operation."""
        print_subheader("Stop DM Operation")
        
        if not self.dm_sender.is_running():
            print_warning("No DM operation is currently running")
            pause()
            return
        
        if prompt_yes_no("Are you sure you want to stop the DM operation?"):
            stopped_count = self.dm_sender.stop_all_tasks()
            print_success(f"Stopped {stopped_count} DM operation(s)")
        
        pause()
    
    def _show_friend_menu(self) -> None:
        """Show the friend request menu."""
        while True:
            clear_screen()
            
            print_header("Friend Requests")
            print_info("Friend requests can help increase DM acceptance rates")
            print_info("Note: Adding friends is optional and not required for direct messaging")
            print()
            
            # Check if friend operation is running
            if self.friend_manager.is_running():
                print_info("A friend request operation is currently running")
                
                tasks = self.friend_manager.get_running_tasks()
                if tasks:
                    task = tasks[0]  # Get the first task
                    
                    print_info(f"Progress: {task['progress']}/{task['user_count']} users")
                    print_info(f"Success: {task['success_count']}, Fails: {task['fail_count']}, Rate Limits: {task['rate_limit_count']}")
                    print_info(f"Running for: {task['duration']:.2f} seconds")
                    print()
                
                print_subheader("Options")
                menu_options = {
                    "1": "Stop Friend Request Operation",
                    "2": "Back to Main Menu"
                }
            else:
                # Show tokens and users count
                print_info(f"Available tokens: {self.token_manager.get_token_count()}")
                print_info(f"Available users: {self.user_manager.get_user_count()}")
                print()
                
                print_subheader("Options")
                menu_options = {
                    "1": "Send Friend Requests to Group",
                    "2": "Send Friend Request to User",
                    "3": "Check Friend Status",
                    "4": "View Friends List",
                    "5": "Back to Main Menu"
                }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if self.friend_manager.is_running():
                if choice == "1":
                    self._stop_friend_operation()
                elif choice == "2":
                    break
                else:
                    print_error("Invalid choice")
                    pause()
            else:
                if choice == "1":
                    self._send_friend_requests_to_group()
                elif choice == "2":
                    self._send_friend_request_to_user()
                elif choice == "3":
                    self._check_friend_status()
                elif choice == "4":
                    self._view_friends_list()
                elif choice == "5":
                    break
                else:
                    print_error("Invalid choice")
                    pause()
    
    def _send_friend_requests_to_group(self) -> None:
        """Send friend requests to a user group."""
        print_subheader("Send Friend Requests to Group")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Get user groups
        groups = self.user_manager.get_user_groups()
        if not groups:
            print_warning("No user groups found")
            print_info("Please create a user group first")
            pause()
            return
        
        # Select group
        group_options = {name: f"{name} ({len(users)} users)" for name, users in groups.items()}
        
        group_name = prompt_options("Select user group", group_options)
        if not group_name:
            return
        
        # Get users in the group
        group_users = self.user_manager.get_users_in_group(group_name)
        if not group_users:
            print_warning(f"No users found in group '{group_name}'")
            pause()
            return
        
        user_ids = [u["user_id"] for u in group_users]
        
        # Confirm sending
        user_count = len(user_ids)
        
        print_info(f"Ready to send friend requests to {user_count} users in group '{group_name}'")
        print_warning("This may trigger Discord's rate limiting mechanisms")
        
        if not prompt_yes_no("Continue with sending?"):
            return
        
        # Progress callback
        def update_progress(current, total, success, fails):
            # Calculate percentage
            percent = int(current / total * 100) if total > 0 else 0
            
            # Create progress bar
            bar_width = 40
            filled_width = int(bar_width * percent / 100)
            bar = "█" * filled_width + "-" * (bar_width - filled_width)
            
            # Print progress
            print(f"\r[{bar}] {percent}% | {current}/{total} | ✓ {success} | ✗ {fails}", end="")
        
        print_info("Starting friend request operation...")
        print()
        
        try:
            # Start the task
            task = self.friend_manager.start_bulk_friend_requests(user_ids, update_progress)
            
            if task:
                # Wait for the task to complete
                result = self.loop.run_until_complete(task)
                
                print("\n")  # Add newline after progress bar
                
                if result["status"] == "complete":
                    print_success(f"Friend request operation completed: {result['success_count']} successful, {result['fail_count']} failed")
                else:
                    print_error(f"Friend request operation failed: {result['message']}")
            else:
                print_error("Failed to start friend request operation")
        
        except Exception as e:
            logger.error(f"Error during friend request operation: {e}")
            print_error(f"Error during friend request operation: {e}")
        
        pause()
    
    def _send_friend_request_to_user(self) -> None:
        """Send a friend request to a specific user."""
        print_subheader("Send Friend Request to User")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Get user ID
        user_id_input = prompt_input("Enter Discord user ID (or search by username)")
        if not user_id_input:
            return
        
        # Check if input is a user ID or search term
        user = None
        if user_id_input.isdigit() and len(user_id_input) >= 17:
            # It's probably a user ID
            user = self.user_manager.get_user_by_discord_id(user_id_input)
        else:
            # Search by username
            users = self.user_manager.get_all_users()
            matching_users = [u for u in users if user_id_input.lower() in u["username"].lower()]
            
            if not matching_users:
                print_warning(f"No users found matching '{user_id_input}'")
                pause()
                return
            
            # If multiple matches, let user select
            if len(matching_users) > 1:
                user_options = {}
                for u in matching_users:
                    user_options[u["user_id"]] = f"{u['username']} ({u['user_id']})"
                
                selected_id = prompt_options("Select user", user_options)
                if not selected_id:
                    return
                
                user = self.user_manager.get_user_by_discord_id(selected_id)
            else:
                user = matching_users[0]
        
        if not user:
            # If still no user found, ask to add it
            if prompt_yes_no(f"User '{user_id_input}' not found. Add it?"):
                success, _ = self.user_manager.add_user(user_id_input)
                if success:
                    user = self.user_manager.get_user_by_discord_id(user_id_input)
                    print_success(f"User '{user_id_input}' added")
                else:
                    print_error(f"Failed to add user '{user_id_input}'")
                    pause()
                    return
            else:
                return
        
        # Select token
        token_options = {
            "auto": "Auto-select token",
        }
        
        for token in self.token_manager.get_all_tokens():
            token_options[token["id"]] = f"{token['alias']} ({token['token'][:10]}...)"
        
        token_choice = prompt_options("Select token to use", token_options)
        if not token_choice:
            return
        
        # Get the token if not auto-select
        token = None
        if token_choice != "auto":
            token_data = self.token_manager.get_token(token_choice)
            if token_data:
                token = token_data["token"]
        else:
            token = self.token_manager.get_next_token()
            if not token:
                print_error("No available tokens")
                pause()
                return
        
        # Confirm sending
        print_info(f"Ready to send friend request to user '{user['username']}' ({user['user_id']})")
        
        if not prompt_yes_no("Send friend request?"):
            return
        
        # Send the friend request
        print_info(f"Sending friend request to user '{user['username']}'...")
        
        try:
            success, status, metadata = self.loop.run_until_complete(
                self.friend_manager.send_friend_request(user["user_id"], token)
            )
            
            if success:
                print_success("Friend request sent successfully")
                
                # Track statistics
                self.stats_manager.track_friend_request(user["user_id"], "sent", token, metadata)
            else:
                print_error(f"Failed to send friend request: {status}")
                
                # Check if rate limited
                if metadata and metadata.get("error_type") == "rate_limited":
                    retry_after = metadata.get("retry_after", 300)
                    print_warning(f"Rate limited. Try again in {retry_after} seconds")
                    
                    # Set token cooldown
                    self.token_manager.set_token_cooldown(token, retry_after)
        
        except Exception as e:
            logger.error(f"Error sending friend request: {e}")
            print_error(f"Error sending friend request: {e}")
        
        pause()

    def _check_friend_status(self) -> None:
        """Check friendship status with a user."""
        print_subheader("Check Friend Status")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Get user ID
        user_id = prompt_user_id("Enter Discord user ID")
        if not user_id:
            return
        
        # Select token
        token_options = {
            "auto": "Auto-select token",
        }
        
        for token in self.token_manager.get_all_tokens():
            token_options[token["id"]] = f"{token['alias']} ({token['token'][:10]}...)"
        
        token_choice = prompt_options("Select token to use", token_options)
        if not token_choice:
            return
        
        # Get the token if not auto-select
        token = None
        if token_choice != "auto":
            token_data = self.token_manager.get_token(token_choice)
            if token_data:
                token = token_data["token"]
        else:
            token = self.token_manager.get_next_token()
            if not token:
                print_error("No available tokens")
                pause()
                return
        
        # Check status
        print_info(f"Checking friend status for user ID {user_id}...")
        
        try:
            is_friend, status, metadata = self.loop.run_until_complete(
                self.friend_manager.check_friend_status(user_id, token)
            )
            
            if metadata and "relationship_type" in metadata:
                rel_type = metadata["relationship_type"]
                if rel_type == "friend":
                    print_success(f"User {user_id} is a friend")
                elif rel_type == "incoming_request":
                    print_info(f"User {user_id} has sent you a friend request")
                elif rel_type == "outgoing_request":
                    print_info(f"You have sent a friend request to user {user_id}")
                elif rel_type == "blocked":
                    print_warning(f"User {user_id} is blocked")
                else:
                    print_info(f"No relationship with user {user_id}")
            else:
                print_info(status)
        
        except Exception as e:
            logger.error(f"Error checking friend status: {e}")
            print_error(f"Error checking friend status: {e}")
        
        pause()
    
    def _view_friends_list(self) -> None:
        """View the friends list for a token."""
        print_subheader("View Friends List")
        
        # Check tokens
        if self.token_manager.get_token_count() == 0:
            print_error("No tokens available")
            print_info("Please add tokens first")
            pause()
            return
        
        # Select token
        token_options = {}
        for token in self.token_manager.get_all_tokens():
            token_options[token["id"]] = f"{token['alias']} ({token['token'][:10]}...)"
        
        token_id = prompt_options("Select token", token_options)
        if not token_id:
            return
        
        token_data = self.token_manager.get_token(token_id)
        if not token_data:
            print_error("Token not found")
            pause()
            return
        
        # Get friends list
        print_info(f"Getting friends list for token '{token_data['alias']}'...")
        
        try:
            success, friends, error = self.loop.run_until_complete(
                self.friend_manager.get_friends_list(token_data["token"])
            )
            
            if success:
                if friends:
                    print_info(f"Found {len(friends)} friends:")
                    
                    friend_data = []
                    for friend in friends:
                        friend_data.append([
                            friend["user_id"],
                            friend["username"],
                            friend.get("discriminator", "")
                        ])
                    
                    print(tabulate(
                        friend_data,
                        headers=["User ID", "Username", "Discriminator"],
                        tablefmt="grid"
                    ))
                else:
                    print_info("No friends found")
            else:
                print_error(f"Failed to get friends list: {error}")
        
        except Exception as e:
            logger.error(f"Error getting friends list: {e}")
            print_error(f"Error getting friends list: {e}")
        
        pause()
    
    def _stop_friend_operation(self) -> None:
        """Stop the running friend request operation."""
        print_subheader("Stop Friend Request Operation")
        
        if not self.friend_manager.is_running():
            print_warning("No friend request operation is currently running")
            pause()
            return
        
        if prompt_yes_no("Are you sure you want to stop the friend request operation?"):
            stopped_count = self.friend_manager.stop_all_tasks()
            print_success(f"Stopped {stopped_count} friend request operation(s)")
        
        pause()
    
    def _show_stats_menu(self) -> None:
        """Show the statistics menu."""
        while True:
            clear_screen()
            
            print_header("Statistics")
            
            # Get statistics
            message_stats = self.stats_manager.get_message_stats()
            user_stats = self.stats_manager.get_user_stats()
            session_stats = self.stats_manager.get_session_stats()
            
            # Display message statistics
            print_subheader("Message Statistics")
            
            message_data = [
                ["Total Sent", message_stats["total_sent"]],
                ["Successful", message_stats["successful"]],
                ["Failed", message_stats["failed"]],
                ["Rate Limited", message_stats["rate_limited"]],
                ["Success Rate", f"{message_stats['success_rate']}%"],
                ["Responses", message_stats["responses"]],
                ["Response Rate", f"{message_stats['response_rate']}%"],
                ["Last Hour", message_stats["last_hour"]],
                ["Last 24 Hours", message_stats["last_24_hours"]]
            ]
            
            print(tabulate(message_data, tablefmt="simple"))
            print()
            
            # Display user statistics
            print_subheader("User Statistics")
            
            user_data = [
                ["Users Messaged", user_stats["messaged"]],
                ["Users Responded", user_stats["responded"]],
                ["Response Rate", f"{user_stats['response_rate']}%"],
                ["Friend Requests Accepted", user_stats["friends_accepted"]],
                ["Friend Requests Rejected", user_stats["friends_rejected"]],
                ["Friend Acceptance Rate", f"{user_stats['friend_acceptance_rate']}%"]
            ]
            
            print(tabulate(user_data, tablefmt="simple"))
            print()
            
            # Display session statistics
            print_subheader("Session Statistics")
            
            session_data = [
                ["Session ID", session_stats["session_id"]],
                ["Session Start", session_stats["session_start"]],
                ["Session Duration", f"{session_stats['session_duration']} minutes"]
            ]
            
            print(tabulate(session_data, tablefmt="simple"))
            print()
            
            print_subheader("Options")
            menu_options = {
                "1": "Export Statistics",
                "2": "Reset Statistics",
                "3": "Back to Main Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._export_statistics()
            elif choice == "2":
                self._reset_statistics()
            elif choice == "3":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _export_statistics(self) -> None:
        """Export statistics to a file."""
        print_subheader("Export Statistics")
        
        filepath = prompt_input("Enter filepath")
        if not filepath:
            return
        
        include_history = prompt_yes_no("Include detailed message history?", default=False)
        
        print_info("Exporting statistics...")
        success = self.stats_manager.export_stats_to_file(filepath, include_history)
        
        if success:
            print_success(f"Statistics exported to {filepath}")
        else:
            print_error("Failed to export statistics")
        
        pause()
    
    def _reset_statistics(self) -> None:
        """Reset statistics."""
        print_subheader("Reset Statistics")
        
        options = {
            "session": "Reset current session statistics only",
            "all": "Reset all statistics"
        }
        
        choice = prompt_options("Select reset option", options)
        if not choice:
            return
        
        if choice == "session":
            if prompt_yes_no("Are you sure you want to reset session statistics?"):
                self.stats_manager.reset_stats(session_only=True)
                print_success("Session statistics reset")
        elif choice == "all":
            if prompt_yes_no("Are you sure you want to reset ALL statistics? This cannot be undone!"):
                self.stats_manager.reset_stats(session_only=False)
                print_success("All statistics reset")
        
        pause()
    
    def _show_settings_menu(self) -> None:
        """Show the settings menu."""
        while True:
            clear_screen()
            
            print_header("Settings")
            
            # Get current settings
            rate_limits = self.config.get("rate_limits", {})
            dm_settings = self.config.get("dm_settings", {})
            logging_settings = self.config.get("logging", {})
            
            # Display current settings
            print_subheader("Rate Limits")
            
            rate_limit_data = [
                ["Messages Per Minute", rate_limits.get("messages_per_minute", 5)],
                ["Friend Requests Per Minute", rate_limits.get("friend_requests_per_minute", 2)],
                ["Cooldown Period (seconds)", rate_limits.get("cooldown_period", 300)],
                ["Jitter Percent", rate_limits.get("jitter_percent", 20)],
                ["Auto Delay Enabled", "Yes" if rate_limits.get("auto_delay_enabled", True) else "No"]
            ]
            
            print(tabulate(rate_limit_data, tablefmt="simple"))
            print()
            
            print_subheader("DM Settings")
            
            dm_settings_data = [
                ["Retry Failed", "Yes" if dm_settings.get("retry_failed", True) else "No"],
                ["Max Retries", dm_settings.get("max_retries", 3)],
                ["Track Statistics", "Yes" if dm_settings.get("track_statistics", True) else "No"]
            ]
            
            print(tabulate(dm_settings_data, tablefmt="simple"))
            print()
            
            print_subheader("Logging Settings")
            
            logging_data = [
                ["Log Level", logging_settings.get("level", "INFO")],
                ["File Enabled", "Yes" if logging_settings.get("file_enabled", True) else "No"],
                ["File Path", logging_settings.get("file_path", "logs/discord_dm_tool.log")],
                ["Console Enabled", "Yes" if logging_settings.get("console_enabled", True) else "No"]
            ]
            
            print(tabulate(logging_data, tablefmt="simple"))
            print()
            
            print_subheader("Options")
            menu_options = {
                "1": "Edit Rate Limits",
                "2": "Edit DM Settings",
                "3": "Edit Logging Settings",
                "4": "Back to Main Menu"
            }
            
            for key, value in menu_options.items():
                print_color(f"  {key}. {value}")
            
            print()
            choice = prompt_input("Enter your choice")
            
            if choice == "1":
                self._edit_rate_limits()
            elif choice == "2":
                self._edit_dm_settings()
            elif choice == "3":
                self._edit_logging_settings()
            elif choice == "4":
                break
            else:
                print_error("Invalid choice")
                pause()
    
    def _edit_rate_limits(self) -> None:
        """Edit rate limit settings."""
        print_subheader("Edit Rate Limits")
        
        rate_limits = self.config.get("rate_limits", {})
        
        # Messages per minute
        current = rate_limits.get("messages_per_minute", 5)
        new_value = prompt_integer(
            f"Messages per minute (current: {current})",
            min_value=1,
            max_value=60,
            default=current
        )
        if new_value is not None:
            rate_limits["messages_per_minute"] = new_value
        
        # Friend requests per minute
        current = rate_limits.get("friend_requests_per_minute", 2)
        new_value = prompt_integer(
            f"Friend requests per minute (current: {current})",
            min_value=1,
            max_value=30,
            default=current
        )
        if new_value is not None:
            rate_limits["friend_requests_per_minute"] = new_value
        
        # Cooldown period
        current = rate_limits.get("cooldown_period", 300)
        new_value = prompt_integer(
            f"Cooldown period in seconds (current: {current})",
            min_value=10,
            max_value=3600,
            default=current
        )
        if new_value is not None:
            rate_limits["cooldown_period"] = new_value
        
        # Jitter percent
        current = rate_limits.get("jitter_percent", 20)
        new_value = prompt_integer(
            f"Jitter percent (current: {current})",
            min_value=0,
            max_value=50,
            default=current
        )
        if new_value is not None:
            rate_limits["jitter_percent"] = new_value
        
        # Auto delay
        current = rate_limits.get("auto_delay_enabled", True)
        new_value = prompt_yes_no(
            f"Enable auto delay (current: {'Yes' if current else 'No'})",
            default=current
        )
        rate_limits["auto_delay_enabled"] = new_value
        
        # Update config
        self.config["rate_limits"] = rate_limits
        
        print_success("Rate limit settings updated")
        pause()
    
    def _edit_dm_settings(self) -> None:
        """Edit DM settings."""
        print_subheader("Edit DM Settings")
        
        dm_settings = self.config.get("dm_settings", {})
        
        # Retry failed
        current = dm_settings.get("retry_failed", True)
        new_value = prompt_yes_no(
            f"Retry failed messages (current: {'Yes' if current else 'No'})",
            default=current
        )
        dm_settings["retry_failed"] = new_value
        
        # Max retries
        if new_value:
            current = dm_settings.get("max_retries", 3)
            new_value = prompt_integer(
                f"Max retries (current: {current})",
                min_value=1,
                max_value=10,
                default=current
            )
            if new_value is not None:
                dm_settings["max_retries"] = new_value
        
        # Track statistics
        current = dm_settings.get("track_statistics", True)
        new_value = prompt_yes_no(
            f"Track statistics (current: {'Yes' if current else 'No'})",
            default=current
        )
        dm_settings["track_statistics"] = new_value
        
        # Update config
        self.config["dm_settings"] = dm_settings
        
        print_success("DM settings updated")
        pause()
    
    def _edit_logging_settings(self) -> None:
        """Edit logging settings."""
        print_subheader("Edit Logging Settings")
        
        logging_settings = self.config.get("logging", {})
        
        # Log level
        current = logging_settings.get("level", "INFO")
        log_level_options = {
            "DEBUG": "DEBUG (most verbose)",
            "INFO": "INFO (recommended)",
            "WARNING": "WARNING (less verbose)",
            "ERROR": "ERROR (least verbose)",
            "CRITICAL": "CRITICAL (errors only)"
        }
        
        new_value = prompt_options(
            "Select log level",
            log_level_options,
            default=current
        )
        if new_value:
            logging_settings["level"] = new_value
        
        # File enabled
        current = logging_settings.get("file_enabled", True)
        new_value = prompt_yes_no(
            f"Enable file logging (current: {'Yes' if current else 'No'})",
            default=current
        )
        logging_settings["file_enabled"] = new_value
        
        # File path
        if new_value:
            current = logging_settings.get("file_path", "logs/discord_dm_tool.log")
            new_value = prompt_input(
                f"Log file path (current: {current})",
                default=current
            )
            if new_value:
                logging_settings["file_path"] = new_value
        
        # Console enabled
        current = logging_settings.get("console_enabled", True)
        new_value = prompt_yes_no(
            f"Enable console logging (current: {'Yes' if current else 'No'})",
            default=current
        )
        logging_settings["console_enabled"] = new_value
        
        # Update config
        self.config["logging"] = logging_settings
        
        # Apply new settings
        if prompt_yes_no("Apply new logging settings now?"):
            from utils.logger import setup_logger
            setup_logger(self.config)
        
        print_success("Logging settings updated")
        pause()