"""
Token management for Discord Mass DM Tool.

This module provides functionality for managing Discord tokens, including:
- Adding, removing, and validating tokens
- Rotating tokens for load balancing
- Storing token metadata
"""

import logging
import time
from typing import Dict, List, Optional, Tuple, Any

from utils.helpers import validate_token, generate_random_id

logger = logging.getLogger("discord_dm_tool")


class TokenManager:
    """
    Manages Discord tokens for the DM tool.
    
    Handles token validation, token rotation, and token metadata.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the TokenManager.
        
        Args:
            config (Dict[str, Any]): The application configuration.
        """
        self.config = config
        self.tokens = self._load_tokens_from_config()
        self.current_token_index = 0
        self.token_usage = {}  # Maps token to number of uses
        self.token_cooldowns = {}  # Maps token to cooldown end time
        
        logger.debug(f"TokenManager initialized with {len(self.tokens)} tokens")
    
    def _load_tokens_from_config(self) -> List[Dict[str, Any]]:
        """
        Load tokens from the configuration.
        
        Returns:
            List[Dict[str, Any]]: List of token dictionaries.
        """
        tokens = self.config.get("tokens", [])
        
        # Convert simple token strings to token dictionaries if needed
        for i, token in enumerate(tokens):
            if isinstance(token, str):
                tokens[i] = {
                    "id": generate_random_id(),
                    "token": token,
                    "alias": f"Token {i+1}",
                    "added_at": time.time(),
                    "metadata": {}
                }
        
        return tokens
    
    def add_token(self, token: str, alias: Optional[str] = None, validate: bool = True) -> Tuple[bool, Optional[str]]:
        """
        Add a new token to the manager.
        
        Args:
            token (str): The Discord token to add.
            alias (Optional[str], optional): A human-readable alias for the token. Defaults to None.
            validate (bool, optional): Whether to validate the token before adding. Defaults to True.
            
        Returns:
            Tuple[bool, Optional[str]]: A tuple containing:
                - Boolean indicating success or failure
                - Error message if validation failed, None otherwise
        """
        # Check if token already exists
        if any(t["token"] == token for t in self.tokens):
            return False, "Token already exists"
        
        # Validate token if required
        if validate:
            is_valid, user_data = validate_token(token)
            if not is_valid:
                return False, "Invalid token"
            
            # Use user data for alias if not provided
            if not alias and user_data:
                username = user_data.get("username", "")
                discriminator = user_data.get("discriminator", "")
                if discriminator:
                    alias = f"{username}#{discriminator}"
                else:
                    alias = username
        
        # If no alias provided or validation not requested, use a default
        if not alias:
            alias = f"Token {len(self.tokens) + 1}"
        
        # Create token entry
        token_entry = {
            "id": generate_random_id(),
            "token": token,
            "alias": alias,
            "added_at": time.time(),
            "metadata": {}
        }
        
        # If validation was successful and we have user data, add it to metadata
        if validate and is_valid and user_data:
            token_entry["metadata"] = {
                "user_id": user_data.get("id"),
                "username": user_data.get("username"),
                "discriminator": user_data.get("discriminator", ""),
                "avatar": user_data.get("avatar"),
                "email": user_data.get("email"),
                "flags": user_data.get("flags", 0)
            }
        
        # Add to token list
        self.tokens.append(token_entry)
        
        # Update configuration
        self.config["tokens"] = self.tokens
        
        logger.info(f"Added token with alias '{alias}'")
        return True, None
    
    def remove_token(self, token_id: str) -> bool:
        """
        Remove a token from the manager.
        
        Args:
            token_id (str): The ID of the token to remove.
            
        Returns:
            bool: True if the token was removed, False otherwise.
        """
        original_length = len(self.tokens)
        self.tokens = [t for t in self.tokens if t["id"] != token_id]
        
        # Check if a token was removed
        if len(self.tokens) < original_length:
            # Update configuration
            self.config["tokens"] = self.tokens
            logger.info(f"Removed token with ID '{token_id}'")
            return True
        
        logger.warning(f"No token found with ID '{token_id}'")
        return False
    
    def get_token(self, token_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a token by its ID.
        
        Args:
            token_id (str): The ID of the token to get.
            
        Returns:
            Optional[Dict[str, Any]]: The token dictionary, or None if not found.
        """
        for token in self.tokens:
            if token["id"] == token_id:
                return token
        return None
    
    def get_next_token(self) -> Optional[str]:
        """
        Get the next available token, using round-robin rotation.
        
        Returns:
            Optional[str]: The next available token, or None if no tokens are available.
        """
        if not self.tokens:
            return None
        
        # Calculate the number of tokens
        token_count = len(self.tokens)
        
        # Get the next token index, starting from the current index
        for _ in range(token_count):
            # Get the token at the current index
            token = self.tokens[self.current_token_index]
            
            # Move to the next index for the next call, wrapping around if necessary
            self.current_token_index = (self.current_token_index + 1) % token_count
            
            # Check if this token is on cooldown
            token_id = token["id"]
            if token_id in self.token_cooldowns:
                # If cooldown has expired, remove it
                if self.token_cooldowns[token_id] <= time.time():
                    del self.token_cooldowns[token_id]
                else:
                    # Token is still on cooldown, try the next one
                    continue
            
            # Token is available, use it
            token_str = token["token"]
            
            # Update usage statistics
            if token_str in self.token_usage:
                self.token_usage[token_str] += 1
            else:
                self.token_usage[token_str] = 1
            
            return token_str
        
        # If we get here, all tokens are on cooldown
        return None
    
    def set_token_cooldown(self, token: str, cooldown_seconds: int) -> None:
        """
        Set a cooldown period for a token.
        
        Args:
            token (str): The token to set the cooldown for.
            cooldown_seconds (int): The cooldown period in seconds.
        """
        # Find the token ID
        token_id = None
        for t in self.tokens:
            if t["token"] == token:
                token_id = t["id"]
                break
        
        if token_id:
            # Calculate cooldown end time
            end_time = time.time() + cooldown_seconds
            self.token_cooldowns[token_id] = end_time
            
            logger.debug(f"Token with ID '{token_id}' put on cooldown for {cooldown_seconds} seconds")
    
    def validate_all_tokens(self) -> Dict[str, bool]:
        """
        Validate all tokens in the manager.
        
        Returns:
            Dict[str, bool]: A dictionary mapping token IDs to validation results.
        """
        results = {}
        
        for token in self.tokens:
            token_id = token["id"]
            token_str = token["token"]
            
            is_valid, user_data = validate_token(token_str)
            results[token_id] = is_valid
            
            # Update metadata if validation was successful
            if is_valid and user_data:
                token["metadata"] = {
                    "user_id": user_data.get("id"),
                    "username": user_data.get("username"),
                    "discriminator": user_data.get("discriminator", ""),
                    "avatar": user_data.get("avatar"),
                    "email": user_data.get("email"),
                    "flags": user_data.get("flags", 0),
                    "last_validated": time.time()
                }
        
        # Update configuration
        self.config["tokens"] = self.tokens
        
        return results
    
    def get_all_tokens(self) -> List[Dict[str, Any]]:
        """
        Get all tokens in the manager.
        
        Returns:
            List[Dict[str, Any]]: List of all token dictionaries.
        """
        return self.tokens.copy()
    
    def get_token_count(self) -> int:
        """
        Get the number of tokens in the manager.
        
        Returns:
            int: The number of tokens.
        """
        return len(self.tokens)
    
    def clear_all_tokens(self) -> None:
        """
        Clear all tokens from the manager.
        """
        self.tokens = []
        self.token_usage = {}
        self.token_cooldowns = {}
        self.current_token_index = 0
        
        # Update configuration
        self.config["tokens"] = []
        
        logger.info("All tokens cleared")
    
    def import_tokens_from_file(self, filepath: str) -> Tuple[int, int]:
        """
        Import tokens from a file.
        
        The file should contain one token per line.
        
        Args:
            filepath (str): The path to the file.
            
        Returns:
            Tuple[int, int]: A tuple containing:
                - Number of successfully imported tokens
                - Number of failed imports
        """
        success_count = 0
        fail_count = 0
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    token = line.strip()
                    if not token:
                        continue
                    
                    success, _ = self.add_token(token)
                    if success:
                        success_count += 1
                    else:
                        fail_count += 1
            
            logger.info(f"Imported {success_count} tokens from file, {fail_count} failed")
            return success_count, fail_count
        
        except Exception as e:
            logger.error(f"Failed to import tokens from file: {e}")
            return success_count, fail_count

    def cache_captcha_solution(self, token: str, captcha_solution: str) -> None:
        """
        缓存已成功解决的验证码解决方案
        
        Args:
            token (str): Discord令牌
            captcha_solution (str): 验证码解决方案
        """
        # 查找令牌数据
        token_data = None
        for t in self.tokens:
            if t.get("token") == token:
                token_data = t
                break
        
        if not token_data:
            return
        
        # 如果令牌数据没有captcha_solutions字段，创建它
        if "captcha_solutions" not in token_data:
            token_data["captcha_solutions"] = {}
        
        # 存储验证码解决方案及其最后使用时间
        token_data["captcha_solutions"]["current"] = {
            "solution": captcha_solution,
            "timestamp": int(time.time())
        }
        
        # 保存更新后的令牌数据
        self._save_tokens()
        
        logger.info(f"Cached captcha solution for token: {self._mask_token(token)}")

    def get_cached_captcha_solution(self, token: str) -> Optional[str]:
        """
        获取缓存的验证码解决方案
        
        Args:
            token (str): Discord令牌
        
        Returns:
            Optional[str]: 缓存的验证码解决方案，如果没有则返回None
        """
        # 查找令牌数据
        token_data = None
        for t in self.tokens:
            if t.get("token") == token:
                token_data = t
                break
        
        if not token_data or "captcha_solutions" not in token_data:
            return None
        
        # 检查是否有当前解决方案
        current_solution = token_data["captcha_solutions"].get("current")
        if not current_solution:
            return None
        
        # 检查解决方案是否过期（超过30分钟）
        solution_age = int(time.time()) - current_solution.get("timestamp", 0)
        if solution_age > 1800:  # 30分钟 = 1800秒
            logger.info(f"Captcha solution for token {self._mask_token(token)} has expired")
            return None
        
        logger.info(f"Using cached captcha solution for token: {self._mask_token(token)}")
        return current_solution.get("solution")

    def _mask_token(self, token: str) -> str:
        """
        掩盖令牌以便安全地记录日志
        
        Args:
            token (str): 完整的Discord令牌
        
        Returns:
            str: 掩盖的令牌，只显示前4个和后4个字符
        """
        if len(token) <= 8:
            return "****"
        return token[:4] + "****" + token[-4:]