"""
Message template management for Discord Mass DM Tool.

This module provides functionality for managing message templates, including:
- Creating, editing, and deleting templates
- Loading and saving templates from/to configuration
- Formatting templates with variables
"""

import logging
import time
from typing import Dict, List, Optional, Any, Tuple

from utils.helpers import extract_template_variables, format_message, generate_random_id

logger = logging.getLogger("discord_dm_tool")


class MessageManager:
    """
    Manages message templates for the DM tool.
    
    Handles template storage, validation, and formatting.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the MessageManager.
        
        Args:
            config (Dict[str, Any]): The application configuration.
        """
        self.config = config
        self.templates = self._load_templates_from_config()
        
        logger.debug(f"MessageManager initialized with {len(self.templates)} templates")
    
    def _load_templates_from_config(self) -> List[Dict[str, Any]]:
        """
        Load message templates from the configuration.
        
        Returns:
            List[Dict[str, Any]]: List of template dictionaries.
        """
        templates = self.config.get("message_templates", [])
        
        # Ensure all templates have required fields
        for i, template in enumerate(templates):
            if "id" not in template:
                template["id"] = generate_random_id()
            
            if "created_at" not in template:
                template["created_at"] = time.time()
            
            if "variables" not in template and "content" in template:
                template["variables"] = extract_template_variables(template["content"])
        
        return templates
    
    def add_template(self, name: str, content: str) -> str:
        """
        Add a new message template.
        
        Args:
            name (str): The name of the template.
            content (str): The content of the template.
            
        Returns:
            str: The ID of the new template.
        """
        # Extract variables from the template
        variables = extract_template_variables(content)
        
        # Create template entry
        template_id = generate_random_id()
        template = {
            "id": template_id,
            "name": name,
            "content": content,
            "variables": variables,
            "created_at": time.time(),
            "updated_at": time.time()
        }
        
        # Add to templates list
        self.templates.append(template)
        
        # Update configuration
        self.config["message_templates"] = self.templates
        
        logger.info(f"Added message template '{name}' with ID '{template_id}'")
        return template_id
    
    def update_template(self, template_id: str, name: Optional[str] = None, content: Optional[str] = None) -> bool:
        """
        Update an existing message template.
        
        Args:
            template_id (str): The ID of the template to update.
            name (Optional[str], optional): The new name for the template. Defaults to None.
            content (Optional[str], optional): The new content for the template. Defaults to None.
            
        Returns:
            bool: True if the template was updated, False otherwise.
        """
        for template in self.templates:
            if template["id"] == template_id:
                if name is not None:
                    template["name"] = name
                
                if content is not None:
                    template["content"] = content
                    template["variables"] = extract_template_variables(content)
                
                template["updated_at"] = time.time()
                
                # Update configuration
                self.config["message_templates"] = self.templates
                
                logger.info(f"Updated message template with ID '{template_id}'")
                return True
        
        logger.warning(f"No message template found with ID '{template_id}'")
        return False
    
    def remove_template(self, template_id: str) -> bool:
        """
        Remove a message template.
        
        Args:
            template_id (str): The ID of the template to remove.
            
        Returns:
            bool: True if the template was removed, False otherwise.
        """
        original_length = len(self.templates)
        self.templates = [t for t in self.templates if t["id"] != template_id]
        
        # Check if a template was removed
        if len(self.templates) < original_length:
            # Update configuration
            self.config["message_templates"] = self.templates
            
            logger.info(f"Removed message template with ID '{template_id}'")
            return True
        
        logger.warning(f"No message template found with ID '{template_id}'")
        return False
    
    def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a template by its ID.
        
        Args:
            template_id (str): The ID of the template to get.
            
        Returns:
            Optional[Dict[str, Any]]: The template dictionary, or None if not found.
        """
        for template in self.templates:
            if template["id"] == template_id:
                return template.copy()
        
        return None
    
    def get_all_templates(self) -> List[Dict[str, Any]]:
        """
        Get all templates.
        
        Returns:
            List[Dict[str, Any]]: List of all template dictionaries.
        """
        return [t.copy() for t in self.templates]
    
    def get_template_count(self) -> int:
        """
        Get the number of templates.
        
        Returns:
            int: The number of templates.
        """
        return len(self.templates)
    
    def format_template(self, template_id: str, variables: Dict[str, str]) -> Optional[str]:
        """
        Format a template with variables.
        
        Args:
            template_id (str): The ID of the template to format.
            variables (Dict[str, str]): Dictionary of variable names and values.
            
        Returns:
            Optional[str]: The formatted message, or None if the template was not found.
        """
        template = self.get_template(template_id)
        if not template:
            return None
        
        return format_message(template["content"], variables)
    
    def validate_template_variables(self, template_id: str, variables: Dict[str, str]) -> Tuple[bool, List[str]]:
        """
        Validate that all required variables are provided for a template.
        
        Args:
            template_id (str): The ID of the template.
            variables (Dict[str, str]): Dictionary of variable names and values.
            
        Returns:
            Tuple[bool, List[str]]: A tuple containing:
                - Boolean indicating if all required variables are provided
                - List of missing variable names
        """
        template = self.get_template(template_id)
        if not template:
            return False, []
        
        required_vars = template["variables"]
        provided_vars = set(variables.keys())
        
        missing_vars = [var for var in required_vars if var not in provided_vars]
        
        return len(missing_vars) == 0, missing_vars
    
    def import_templates_from_file(self, filepath: str) -> Tuple[int, int]:
        """
        Import templates from a JSON file.
        
        Args:
            filepath (str): The path to the file.
            
        Returns:
            Tuple[int, int]: A tuple containing:
                - Number of successfully imported templates
                - Number of failed imports
        """
        import json
        
        success_count = 0
        fail_count = 0
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                templates = json.load(f)
            
            if not isinstance(templates, list):
                templates = [templates]
            
            for template in templates:
                if not isinstance(template, dict):
                    fail_count += 1
                    continue
                
                if "name" not in template or "content" not in template:
                    fail_count += 1
                    continue
                
                # Add the template
                self.add_template(template["name"], template["content"])
                success_count += 1
            
            logger.info(f"Imported {success_count} templates from file, {fail_count} failed")
            return success_count, fail_count
        
        except Exception as e:
            logger.error(f"Failed to import templates from file: {e}")
            return success_count, fail_count
    
    def export_templates_to_file(self, filepath: str) -> bool:
        """
        Export templates to a JSON file.
        
        Args:
            filepath (str): The path to the file.
            
        Returns:
            bool: True if the export was successful, False otherwise.
        """
        import json
        import os
        
        try:
            # Ensure the directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.templates, f, indent=4)
            
            logger.info(f"Exported {len(self.templates)} templates to {filepath}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to export templates to file: {e}")
            return False