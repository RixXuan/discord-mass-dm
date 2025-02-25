"""
User interaction prompts for Discord Mass DM Tool.

This module provides functions for prompting the user for input and displaying information.
"""

import os
import sys
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable, Union
import re

from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.validation import Validator, ValidationError
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.shortcuts import clear, radiolist_dialog, checkboxlist_dialog
from colorama import Fore, Style, init as colorama_init

# Initialize colorama
colorama_init(autoreset=True)

logger = logging.getLogger("discord_dm_tool")


def print_color(text: str, color: str = Fore.WHITE, bold: bool = False, end: str = "\n") -> None:
    """
    Print colored text to the console.
    
    Args:
        text (str): The text to print.
        color (str, optional): The color to use. Defaults to Fore.WHITE.
        bold (bool, optional): Whether to print in bold. Defaults to False.
        end (str, optional): The end character. Defaults to "\\n".
    """
    style = Style.BRIGHT if bold else ""
    print(f"{color}{style}{text}{Style.RESET_ALL}", end=end)


def print_header(text: str) -> None:
    """
    Print a header to the console.
    
    Args:
        text (str): The header text.
    """
    width = os.get_terminal_size().columns
    print_color("=" * width, Fore.CYAN)
    print_color(text.center(width), Fore.CYAN, bold=True)
    print_color("=" * width, Fore.CYAN)


def print_subheader(text: str) -> None:
    """
    Print a subheader to the console.
    
    Args:
        text (str): The subheader text.
    """
    width = os.get_terminal_size().columns
    print_color("-" * width, Fore.YELLOW)
    print_color(text, Fore.YELLOW, bold=True)
    print_color("-" * width, Fore.YELLOW)


def print_success(text: str) -> None:
    """
    Print a success message to the console.
    
    Args:
        text (str): The success message.
    """
    print_color(f"[+] {text}", Fore.GREEN)


def print_info(text: str) -> None:
    """
    Print an info message to the console.
    
    Args:
        text (str): The info message.
    """
    print_color(f"[*] {text}", Fore.BLUE)


def print_warning(text: str) -> None:
    """
    Print a warning message to the console.
    
    Args:
        text (str): The warning message.
    """
    print_color(f"[!] {text}", Fore.YELLOW)


def print_error(text: str) -> None:
    """
    Print an error message to the console.
    
    Args:
        text (str): The error message.
    """
    print_color(f"[!] {text}", Fore.RED, bold=True)


def clear_screen() -> None:
    """Clear the console screen."""
    clear()


def prompt_input(message: str, default: str = "", validator: Optional[Validator] = None,
                completer: Optional[WordCompleter] = None) -> str:
    """
    Prompt the user for input.
    
    Args:
        message (str): The prompt message.
        default (str, optional): The default value. Defaults to "".
        validator (Optional[Validator], optional): The input validator. Defaults to None.
        completer (Optional[WordCompleter], optional): The input completer. Defaults to None.
        
    Returns:
        str: The user input.
    """
    try:
        return prompt(
            HTML(f"<ansiyellow>{message}</ansiyellow> "),
            default=default,
            validator=validator,
            completer=completer
        )
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return ""


def prompt_yes_no(message: str, default: bool = True) -> bool:
    """
    Prompt the user for a yes/no answer.
    
    Args:
        message (str): The prompt message.
        default (bool, optional): The default value. Defaults to True.
        
    Returns:
        bool: True if the user answered yes, False otherwise.
    """
    default_text = "Y/n" if default else "y/N"
    while True:
        try:
            response = prompt(HTML(f"<ansiyellow>{message} [{default_text}]</ansiyellow> "))
            
            if not response:
                return default
            
            if response.lower() in ["y", "yes"]:
                return True
            
            if response.lower() in ["n", "no"]:
                return False
            
            print_error("Please enter 'y' or 'n'")
        
        except (KeyboardInterrupt, EOFError):
            print_warning("\nOperation cancelled by user")
            return False


def prompt_options(message: str, options: Dict[str, str], default: Optional[str] = None) -> Optional[str]:
    """
    Prompt the user to select an option from a list.
    
    Args:
        message (str): The prompt message.
        options (Dict[str, str]): Dictionary mapping option values to display texts.
        default (Optional[str], optional): The default option value. Defaults to None.
        
    Returns:
        Optional[str]: The selected option value, or None if cancelled.
    """
    try:
        result = radiolist_dialog(
            title=message,
            values=[(key, value) for key, value in options.items()],
            default=default
        ).run()
        
        return result
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_multi_select(message: str, options: Dict[str, str], 
                      default: Optional[List[str]] = None) -> Optional[List[str]]:
    """
    Prompt the user to select multiple options from a list.
    
    Args:
        message (str): The prompt message.
        options (Dict[str, str]): Dictionary mapping option values to display texts.
        default (Optional[List[str]], optional): List of default selected option values. Defaults to None.
        
    Returns:
        Optional[List[str]]: List of selected option values, or None if cancelled.
    """
    try:
        result = checkboxlist_dialog(
            title=message,
            values=[(key, value) for key, value in options.items()],
            default=default or []
        ).run()
        
        return result
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_filepath(message: str, default: str = "", must_exist: bool = True) -> Optional[str]:
    """
    Prompt the user for a filepath.
    
    Args:
        message (str): The prompt message.
        default (str, optional): The default filepath. Defaults to "".
        must_exist (bool, optional): Whether the file must exist. Defaults to True.
        
    Returns:
        Optional[str]: The filepath, or None if cancelled.
    """
    class FilepathValidator(Validator):
        def validate(self, document):
            filepath = document.text
            
            if not filepath:
                raise ValidationError(message="Filepath cannot be empty")
            
            if must_exist and not os.path.exists(filepath):
                raise ValidationError(message=f"File '{filepath}' does not exist")
    
    try:
        return prompt_input(message, default, FilepathValidator())
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_token(message: str = "Enter Discord token") -> Optional[str]:
    """
    Prompt the user for a Discord token.
    
    Args:
        message (str, optional): The prompt message. Defaults to "Enter Discord token".
        
    Returns:
        Optional[str]: The token, or None if cancelled.
    """
    class TokenValidator(Validator):
        def validate(self, document):
            token = document.text
            
            if not token:
                raise ValidationError(message="Token cannot be empty")
            
            # Basic token format validation
            if not re.match(r'^[A-Za-z0-9._-]+$', token):
                raise ValidationError(message="Invalid token format")
    
    try:
        return prompt_input(message, validator=TokenValidator())
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_user_id(message: str = "Enter Discord user ID") -> Optional[str]:
    """
    Prompt the user for a Discord user ID.
    
    Args:
        message (str, optional): The prompt message. Defaults to "Enter Discord user ID".
        
    Returns:
        Optional[str]: The user ID, or None if cancelled.
    """
    class UserIDValidator(Validator):
        def validate(self, document):
            user_id = document.text
            
            if not user_id:
                raise ValidationError(message="User ID cannot be empty")
            
            # Snowflake ID validation (17-19 digits)
            if not re.match(r'^\d{17,19}$', user_id):
                raise ValidationError(message="Invalid user ID format (should be 17-19 digits)")
    
    try:
        return prompt_input(message, validator=UserIDValidator())
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_server_id(message: str = "Enter Discord server ID") -> Optional[str]:
    """
    Prompt the user for a Discord server ID.
    
    Args:
        message (str, optional): The prompt message. Defaults to "Enter Discord server ID".
        
    Returns:
        Optional[str]: The server ID, or None if cancelled.
    """
    class ServerIDValidator(Validator):
        def validate(self, document):
            server_id = document.text
            
            if not server_id:
                raise ValidationError(message="Server ID cannot be empty")
            
            # Snowflake ID validation (17-19 digits)
            if not re.match(r'^\d{17,19}$', server_id):
                raise ValidationError(message="Invalid server ID format (should be 17-19 digits)")
    
    try:
        return prompt_input(message, validator=ServerIDValidator())
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_message_template(message: str = "Enter message template") -> Optional[str]:
    """
    Prompt the user for a message template.
    
    Args:
        message (str, optional): The prompt message. Defaults to "Enter message template".
        
    Returns:
        Optional[str]: The message template, or None if cancelled.
    """
    class TemplateValidator(Validator):
        def validate(self, document):
            template = document.text
            
            if not template:
                raise ValidationError(message="Template cannot be empty")
            
            if len(template) < 5:
                raise ValidationError(message="Template is too short")
    
    try:
        print_info("You can use variables like {username} in your template")
        print_info("Press Ctrl+D to finish (Ctrl+Z on Windows)")
        print()
        
        lines = []
        try:
            while True:
                line = prompt("... ")
                lines.append(line)
        except (KeyboardInterrupt, EOFError):
            pass
        
        template = "\n".join(lines)
        
        if not template:
            print_warning("Template cannot be empty")
            return None
        
        return template
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_integer(message: str, min_value: Optional[int] = None, 
                 max_value: Optional[int] = None, default: Optional[int] = None) -> Optional[int]:
    """
    Prompt the user for an integer value.
    
    Args:
        message (str): The prompt message.
        min_value (Optional[int], optional): The minimum allowed value. Defaults to None.
        max_value (Optional[int], optional): The maximum allowed value. Defaults to None.
        default (Optional[int], optional): The default value. Defaults to None.
        
    Returns:
        Optional[int]: The integer value, or None if cancelled.
    """
    class IntegerValidator(Validator):
        def validate(self, document):
            value = document.text
            
            if not value and default is None:
                raise ValidationError(message="Value cannot be empty")
            
            if not value:
                return
            
            try:
                num = int(value)
                
                if min_value is not None and num < min_value:
                    raise ValidationError(message=f"Value must be greater than or equal to {min_value}")
                
                if max_value is not None and num > max_value:
                    raise ValidationError(message=f"Value must be less than or equal to {max_value}")
            
            except ValueError:
                raise ValidationError(message="Please enter a valid integer")
    
    try:
        default_str = str(default) if default is not None else ""
        result = prompt_input(message, default_str, IntegerValidator())
        
        if not result and default is not None:
            return default
        
        return int(result) if result else None
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def prompt_float(message: str, min_value: Optional[float] = None, 
               max_value: Optional[float] = None, default: Optional[float] = None) -> Optional[float]:
    """
    Prompt the user for a float value.
    
    Args:
        message (str): The prompt message.
        min_value (Optional[float], optional): The minimum allowed value. Defaults to None.
        max_value (Optional[float], optional): The maximum allowed value. Defaults to None.
        default (Optional[float], optional): The default value. Defaults to None.
        
    Returns:
        Optional[float]: The float value, or None if cancelled.
    """
    class FloatValidator(Validator):
        def validate(self, document):
            value = document.text
            
            if not value and default is None:
                raise ValidationError(message="Value cannot be empty")
            
            if not value:
                return
            
            try:
                num = float(value)
                
                if min_value is not None and num < min_value:
                    raise ValidationError(message=f"Value must be greater than or equal to {min_value}")
                
                if max_value is not None and num > max_value:
                    raise ValidationError(message=f"Value must be less than or equal to {max_value}")
            
            except ValueError:
                raise ValidationError(message="Please enter a valid number")
    
    try:
        default_str = str(default) if default is not None else ""
        result = prompt_input(message, default_str, FloatValidator())
        
        if not result and default is not None:
            return default
        
        return float(result) if result else None
    
    except (KeyboardInterrupt, EOFError):
        print_warning("\nOperation cancelled by user")
        return None


def pause(message: str = "Press Enter to continue...") -> None:
    """
    Pause program execution until the user presses Enter.
    
    Args:
        message (str, optional): The message to display. Defaults to "Press Enter to continue...".
    """
    try:
        input(Fore.CYAN + message + Style.RESET_ALL)
    except (KeyboardInterrupt, EOFError):
        print()