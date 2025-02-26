"""
Direct message sender for Discord Mass DM Tool.

This module provides functionality for sending direct messages to Discord users.
"""

import logging
import time
import asyncio
import random
from typing import Dict, List, Optional, Any, Tuple, Set, Callable

import discord
from discord.ext import commands

import aiohttp
from anticaptchaofficial.hcaptchaproxyless import *

from core.token_manager import TokenManager
from core.user_manager import UserManager
from core.message_manager import MessageManager
from core.stats_manager import StatsManager
from utils.helpers import apply_rate_limit

logger = logging.getLogger("discord_dm_tool")


class DMSender:
    """
    Sends direct messages to Discord users.
    
    Uses Discord tokens to send DMs to users based on configured templates and settings.
    """
    
    def __init__(self, config: Dict[str, Any], token_manager: TokenManager, 
                 user_manager: UserManager, message_manager: MessageManager,
                 stats_manager: StatsManager):
        """
        Initialize the DMSender.
        
        Args:
            config (Dict[str, Any]): The application configuration.
            token_manager (TokenManager): Token manager instance.
            user_manager (UserManager): User manager instance.
            message_manager (MessageManager): Message template manager instance.
            stats_manager (StatsManager): Statistics manager instance.
        """
        self.config = config
        self.token_manager = token_manager
        self.user_manager = user_manager
        self.message_manager = message_manager
        self.stats_manager = stats_manager
        
        # Load settings
        rate_limits = self.config.get("rate_limits", {})
        self.messages_per_minute = rate_limits.get("messages_per_minute", 5)
        self.jitter_percent = rate_limits.get("jitter_percent", 20)
        self.cooldown_period = rate_limits.get("cooldown_period", 300)
        self.auto_delay = rate_limits.get("auto_delay_enabled", True)
        
        dm_settings = self.config.get("dm_settings", {})
        self.retry_failed = dm_settings.get("retry_failed", True)
        self.max_retries = dm_settings.get("max_retries", 3)
        self.track_stats = dm_settings.get("track_statistics", True)
        
        # Track currently running tasks
        self.running = False
        self.tasks = []
        self.current_progress = 0
        self.total_users = 0
        self.success_count = 0
        self.fail_count = 0
        self.rate_limit_count = 0
        
        # Cache to avoid duplicate sends
        self.sent_cache = set()
        
        logger.debug("DMSender initialized")


    async def solve_discord_captcha(self, sitekey, rqdata, rqtoken):
        """
        使用anti-captcha服务解决Discord验证码
        
        Args:
            sitekey (str): hCaptcha的站点密钥
            rqdata (str): 验证码请求数据
            rqtoken (str): 验证码请求令牌
            
        Returns:
            str: 解决的验证码密钥或None如果失败
        """
        logger.info("Attempting to solve captcha using anti-captcha service")
        
        # 将异步操作包装在一个同步函数中，然后从事件循环中调用
        def solve_captcha_sync():
            try:
                # 初始化anti-captcha解算器
                solver = hCaptchaProxyless()
                solver.set_verbose(1)
                # 替换为你的anti-captcha API密钥
                solver.set_key("cca119e5e56cd8ad322a21280be38146")
                solver.set_website_url("https://discord.com")
                solver.set_website_key(sitekey)
                
                # 设置额外的Discord特定数据
                solver.set_enterprise_payload({
                    "rqdata": rqdata,
                })
                
                # 解决验证码
                captcha_key = solver.solve_and_return_solution()
                if captcha_key != 0:
                    logger.info("Captcha successfully solved")
                    return captcha_key
                else:
                    logger.error(f"Error solving captcha: {solver.error_code}")
                    return None
            except Exception as e:
                logger.error(f"Exception during captcha solving: {e}")
                return None
        
        # 在事件循环的执行器中运行同步任务
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, solve_captcha_sync)

    async def send_dm(self, user_id: str, message: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Send a direct message to a Discord user using direct API call with enhanced human simulation."""
        
        logger.info(f"Preparing to send DM to user {user_id}")
        
        # 模拟人类首先查看用户个人资料
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json',
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(100, 115)}.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://discord.com/channels/@me',
            'Origin': 'https://discord.com',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'X-Super-Properties': 'eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiQ2hyb21lIiwiZGV2aWNlIjoiIiwic3lzdGVtX2xvY2FsZSI6ImVuLVVTIiwiYnJvd3Nlcl91c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzExMC4wLjAuMCBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTEwLjAuMC4wIiwib3NfdmVyc2lvbiI6IjEwIiwicmVmZXJyZXIiOiIiLCJyZWZlcnJpbmdfZG9tYWluIjoiIiwicmVmZXJyZXJfY3VycmVudCI6IiIsInJlZmVycmluZ19kb21haW5fY3VycmVudCI6IiIsInJlbGVhc2VfY2hhbm5lbCI6InN0YWJsZSIsImNsaWVudF9idWlsZF9udW1iZXIiOjk5OTk5LCJjbGllbnRfZXZlbnRfc291cmNlIjpudWxsfQ==',
            'X-Discord-Locale': 'en-US',
            'X-Discord-Timezone': 'America/New_York',
            'Cookie': f'locale=en-US; __dcfduid={random.randint(100000, 999999)}; __sdcfduid={random.randint(100000, 999999)}'
        }
        
        # 随机生成会话ID来模拟不同会话
        session_id = ''.join(random.choices('0123456789abcdef', k=32))
        
        try:
            async with aiohttp.ClientSession() as session:
                # 首先，模拟浏览用户个人资料
                logger.info("Simulating profile browsing behavior")
                await asyncio.sleep(random.uniform(2.5, 5.7))
                
                # 随机操作：有时查看用户个人资料
                if random.random() > 0.3:
                    async with session.get(
                        f'https://discord.com/api/v9/users/{user_id}/profile',
                        headers=headers
                    ) as profile_response:
                        logger.info(f"Viewed user profile, status: {profile_response.status}")
                        # 模拟阅读时间
                        await asyncio.sleep(random.uniform(3.1, 8.4))
                
                # 创建DM通道前的"思考时间"
                await asyncio.sleep(random.uniform(2.8, 6.3))
                
                # 第1步: 创建DM通道
                create_dm_url = 'https://discord.com/api/v9/users/@me/channels'
                create_dm_payload = {'recipient_id': user_id}
                
                # 模拟表单填写和提交的时间
                logger.info("Preparing to create DM channel")
                await asyncio.sleep(random.uniform(1.5, 3.2))
                
                # 实际创建DM通道
                async with session.post(
                    create_dm_url, 
                    headers=headers, 
                    json=create_dm_payload
                ) as response:
                    # 处理响应...
                    
                    # 模拟处理验证码重试最多3次
                    max_captcha_retries = 3
                    for captcha_retry in range(max_captcha_retries):
                        if response.status != 200:
                            response_text = await response.text()
                            logger.error(f"Failed to create DM channel: {response_text}")
                            
                            # 检查是否需要验证码
                            try:
                                response_json = json.loads(response_text)
                                if "captcha_key" in response_json and "captcha-required" in response_json["captcha_key"]:
                                    logger.info(f"Captcha challenge detected (attempt {captcha_retry+1}/{max_captcha_retries}), attempting to solve...")
                                    
                                    # 提取验证码数据
                                    captcha_sitekey = response_json.get("captcha_sitekey")
                                    captcha_rqdata = response_json.get("captcha_rqdata")
                                    captcha_rqtoken = response_json.get("captcha_rqtoken")
                                    
                                    # 解决验证码
                                    captcha_key = await self.solve_discord_captcha(
                                        captcha_sitekey, captcha_rqdata, captcha_rqtoken
                                    )
                                    
                                    if captcha_key:
                                        logger.info("Captcha solved, simulating human thinking and verification...")
                                        
                                        # 模拟真实用户在收到验证码后的行为
                                        # 1. 用户看到验证码 - 短暂停顿
                                        await asyncio.sleep(random.uniform(0.8, 2.1))
                                        
                                        # 2. 用户思考和解决验证码 - 长暂停
                                        thinking_time = random.uniform(5.2, 12.8)
                                        logger.info(f"Simulating human solving captcha for {thinking_time:.2f} seconds")
                                        await asyncio.sleep(thinking_time)
                                        
                                        # 3. 用户确认并准备提交 - 短暂停
                                        await asyncio.sleep(random.uniform(1.3, 3.7))
                                        
                                        # 变更一些请求头以模拟新的请求
                                        headers["X-Captcha-Key"] = captcha_key
                                        headers["X-Track"] = f"{random.randint(100000, 999999)}"
                                        headers["X-Discord-Locale"] = random.choice(["en-US", "en-GB"])
                                        
                                        # 重试创建DM通道 - 不同的请求方式
                                        create_dm_with_captcha_url = 'https://discord.com/api/v9/users/@me/channels'
                                        
                                        # 如果这是第二次或更高的尝试，使用不同的参数或方法
                                        if captcha_retry > 0:
                                            # 尝试使用不同端点或添加额外参数
                                            create_dm_payload["_trace"] = f"trace_{random.randint(1000, 9999)}"
                                            
                                            # 尝试更换提交方式
                                            if captcha_retry == 2:  # 第三次尝试时
                                                # 使用延迟提交方式：先验证，然后再创建
                                                logger.info("Using alternative submission flow")
                                                await asyncio.sleep(random.uniform(3.0, 7.0))
                                                
                                                # 先验证验证码
                                                async with session.post(
                                                    'https://discord.com/api/v9/auth/verify-captcha',
                                                    headers=headers,
                                                    json={"captcha_key": captcha_key, "captcha_rqtoken": captcha_rqtoken}
                                                ) as verify_response:
                                                    logger.info(f"Captcha verification response: {verify_response.status}")
                                                    await asyncio.sleep(random.uniform(1.0, 3.0))
                                        
                                        # 尝试重新创建DM通道
                                        async with session.post(
                                            create_dm_with_captcha_url, 
                                            headers=headers, 
                                            json=create_dm_payload
                                        ) as retry_response:
                                            response = retry_response  # 更新响应对象
                                            # 如果成功，跳出循环
                                            if retry_response.status == 200:
                                                logger.info("Successfully created DM channel after captcha")
                                                channel_data = await retry_response.json()
                                                break
                                            else:
                                                # 如果仍然失败，记录错误并尝试下一次循环
                                                error_text = await retry_response.text()
                                                logger.error(f"Failed even with captcha (attempt {captcha_retry+1}): {error_text}")
                                                
                                                # 如果这是最后一次尝试，返回错误
                                                if captcha_retry == max_captcha_retries - 1:
                                                    return False, f"Failed after {max_captcha_retries} captcha attempts: {error_text}", {
                                                        "error_type": "persistent_captcha",
                                                        "status_code": retry_response.status
                                                    }
                                                
                                                # 添加额外的延迟，为下一次尝试做准备
                                                await asyncio.sleep(random.uniform(8.0, 15.0))
                                    else:
                                        return False, "Failed to solve captcha", {"error_type": "captcha_solving_failed"}
                                else:
                                    return False, f"Failed to create DM channel: Status {response.status}", {
                                        "error_type": "channel_creation_failed",
                                        "status_code": response.status
                                    }
                            except json.JSONDecodeError:
                                return False, f"Failed to create DM channel: Status {response.status}", {
                                    "error_type": "channel_creation_failed",
                                    "status_code": response.status
                                }
                        else:
                            # 成功创建DM通道
                            channel_data = await response.json()
                            break
                    
                    channel_id = channel_data.get('id')
                    if not channel_id:
                        return False, "Could not get DM channel ID", {"error_type": "no_channel_id"}
                    
                    # 模拟打开DM通道后思考消息内容的时间
                    logger.info("Channel created, preparing message...")
                    message_preparation_time = random.uniform(5.0, 15.0)
                    logger.info(f"Simulating message composition for {message_preparation_time:.2f} seconds")
                    await asyncio.sleep(message_preparation_time)
                    
                    # 分阶段模拟消息输入（长消息分几次输入）
                    message_length = len(message)
                    typing_speed = random.uniform(2.0, 5.0)  # 字符/秒
                    estimated_typing_time = message_length / typing_speed
                    
                    # 模拟"正在输入"状态
                    typing_url = f'https://discord.com/api/v9/channels/{channel_id}/typing'
                    logger.info(f"Simulating typing for ~{estimated_typing_time:.2f} seconds")
                    
                    # 对于长消息，分段模拟输入
                    typing_intervals = min(int(estimated_typing_time / 8) + 1, 3)  # 最多3段
                    for _ in range(typing_intervals):
                        # 发送"正在输入"信号
                        async with session.post(typing_url, headers=headers) as typing_response:
                            logger.debug(f"Typing signal sent: {typing_response.status}")
                        
                        # 模拟输入一段时间
                        segment_time = min(estimated_typing_time / typing_intervals, 8.0)
                        await asyncio.sleep(segment_time)
                    
                    # 最后的停顿，模拟检查和准备发送
                    await asyncio.sleep(random.uniform(1.0, 3.5))
                    
                    # 第2步: 发送消息
                    send_message_url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
                    message_nonce = str(int(time.time() * 1000))  # 生成nonce以模拟客户端
                    message_payload = {
                        'content': message,
                        'nonce': message_nonce,
                        'tts': False
                    }
                    
                    # 发送消息
                    logger.info("Sending message...")
                    async with session.post(
                        send_message_url, 
                        headers=headers, 
                        json=message_payload
                    ) as msg_response:
                        response_status = msg_response.status
                        response_text = await msg_response.text()
                        
                        # 检查是否成功
                        if response_status == 200 or response_status == 201:
                            message_data = json.loads(response_text) if response_text else {}
                            return True, "Message sent successfully", {
                                "message_id": message_data.get('id'),
                                "channel_id": channel_id,
                                "timestamp": message_data.get('timestamp'),
                                "user_id": user_id
                            }
                        
                        # 处理消息发送验证码，与上面类似的逻辑
                        # 这里省略了，实际代码中应添加与上面类似的验证码处理逻辑
                        
        except Exception as e:
            logger.error(f"Error in send_dm: {str(e)}")
            return False, f"Error: {str(e)}", {"error_type": "exception", "error": str(e)}

    async def send_bulk_dms(self, template_id: str, user_ids: List[str], 
                            variables: Dict[str, str] = None,
                            progress_callback: Optional[Callable[[int, int, int, int], None]] = None,
                            stop_event: Optional[asyncio.Event] = None) -> Dict[str, Any]:
        """
        Send direct messages to multiple Discord users.
        
        Args:
            template_id (str): The ID of the message template to use.
            user_ids (List[str]): List of Discord user IDs to message.
            variables (Dict[str, str], optional): Variables to use in the message template. Defaults to None.
            progress_callback (Optional[Callable[[int, int, int, int], None]], optional): Callback function for progress updates.
                The callback receives (current, total, success_count, fail_count). Defaults to None.
            stop_event (Optional[asyncio.Event], optional): Event to signal stopping the operation. Defaults to None.
            
        Returns:
            Dict[str, Any]: Dictionary with operation statistics.
        """
        # Reset counters
        self.current_progress = 0
        self.total_users = len(user_ids)
        self.success_count = 0
        self.fail_count = 0
        self.rate_limit_count = 0
        self.sent_cache = set()
        self.running = True
        
        # Get template
        template = self.message_manager.get_template(template_id)
        if not template:
            logger.error(f"Template with ID '{template_id}' not found")
            self.running = False
            return {
                "status": "error",
                "message": "Template not found",
                "success_count": 0,
                "fail_count": 0,
                "rate_limit_count": 0
            }
        
        variables = variables or {}
        
        # Validate template variables
        is_valid, missing_vars = self.message_manager.validate_template_variables(template_id, variables)
        if not is_valid:
            logger.error(f"Missing template variables: {', '.join(missing_vars)}")
            self.running = False
            return {
                "status": "error",
                "message": f"Missing template variables: {', '.join(missing_vars)}",
                "success_count": 0,
                "fail_count": 0,
                "rate_limit_count": 0
            }
        
        # Format base message
        base_message = self.message_manager.format_template(template_id, variables)
        
        # Create stop event if not provided
        if stop_event is None:
            stop_event = asyncio.Event()
        
        # Start sending messages
        start_time = time.time()
        
        for i, user_id in enumerate(user_ids):
            if stop_event.is_set():
                logger.info("Bulk DM operation stopped by user")
                break
            
            # Get user
            user = self.user_manager.get_user_by_discord_id(user_id)
            if not user:
                logger.warning(f"User with ID '{user_id}' not found in user manager")
                self.fail_count += 1
                self.current_progress += 1
                
                if progress_callback:
                    progress_callback(self.current_progress, self.total_users, 
                                     self.success_count, self.fail_count)
                continue
            
            # Check if already sent in this session
            if user_id in self.sent_cache:
                logger.debug(f"Skipping user '{user_id}' (already sent in this session)")
                self.current_progress += 1
                
                if progress_callback:
                    progress_callback(self.current_progress, self.total_users, 
                                     self.success_count, self.fail_count)
                continue
            
            # Get token
            token = self.token_manager.get_next_token()
            if token is None:
                logger.error("No tokens available for sending DMs")
                break
            
            # Personalize message if needed
            message = base_message
            if "{username}" in message and "username" not in variables:
                # Use username from user manager
                username = user.get("username", "user")
                message = message.replace("{username}", username)
            
            # Send message
            success, status, metadata = await self.send_dm(user_id, message, token)
            
            if success:
                self.success_count += 1
                logger.info(f"Successfully sent DM to user '{user_id}'")
                
                # Mark as messaged
                self.user_manager.mark_user_as_messaged(user_id, "sent", metadata)
                
                # Add to sent cache
                self.sent_cache.add(user_id)
                
                # Track statistics
                if self.track_stats:
                    self.stats_manager.track_message_sent(user_id, token, "success", metadata)
            
            else:
                # Check if rate limited
                if metadata and metadata.get("error_type") == "rate_limited":
                    self.rate_limit_count += 1
                    retry_after = metadata.get("retry_after", self.cooldown_period)
                    
                    logger.warning(f"Rate limited. Cooling down for {retry_after} seconds")
                    
                    # Set token cooldown
                    self.token_manager.set_token_cooldown(token, retry_after)
                    
                    # Track statistics
                    if self.track_stats:
                        self.stats_manager.track_message_sent(user_id, token, "rate_limited", metadata)
                    
                    # Sleep for a while
                    if stop_event:
                        try:
                            # Sleep but allow for interruption
                            await asyncio.wait_for(stop_event.wait(), timeout=min(retry_after, 5))
                            if stop_event.is_set():
                                break
                        except asyncio.TimeoutError:
                            pass
                    else:
                        await asyncio.sleep(min(retry_after, 5))
                
                else:
                    # Other failure
                    self.fail_count += 1
                    logger.error(f"Failed to send DM to user '{user_id}': {status}")
                    
                    # Mark as failed
                    self.user_manager.mark_user_as_messaged(user_id, "failed", metadata)
                    
                    # Track statistics
                    if self.track_stats:
                        self.stats_manager.track_message_sent(user_id, token, "failed", metadata)
            
            # Update progress
            self.current_progress += 1
            if progress_callback:
                progress_callback(self.current_progress, self.total_users, 
                                 self.success_count, self.fail_count)
            
            # Apply rate limiting
            if self.auto_delay and i < len(user_ids) - 1:
                apply_rate_limit(self.messages_per_minute, self.jitter_percent)
        
        # Operation complete
        end_time = time.time()
        duration = end_time - start_time
        
        self.running = False
        
        return {
            "status": "complete",
            "message": f"Bulk DM operation completed in {duration:.2f} seconds",
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "rate_limit_count": self.rate_limit_count,
            "duration": duration
        }
    
    def start_bulk_dms(self, template_id: str, user_ids: List[str], 
                      variables: Dict[str, str] = None,
                      progress_callback: Optional[Callable[[int, int, int, int], None]] = None) -> asyncio.Task:
        """
        Start sending direct messages to multiple Discord users in the background.
        
        Args:
            template_id (str): The ID of the message template to use.
            user_ids (List[str]): List of Discord user IDs to message.
            variables (Dict[str, str], optional): Variables to use in the message template. Defaults to None.
            progress_callback (Optional[Callable[[int, int, int, int], None]], optional): Callback function for progress updates.
                The callback receives (current, total, success_count, fail_count). Defaults to None.
            
        Returns:
            asyncio.Task: The background task.
        """
        if self.running:
            logger.warning("A bulk DM operation is already running")
            return None
        
        # Create stop event
        stop_event = asyncio.Event()
        
        # Create task
        task = asyncio.create_task(
            self.send_bulk_dms(template_id, user_ids, variables, progress_callback, stop_event)
        )
        
        # Store task and stop event
        self.tasks.append({
            "task": task,
            "stop_event": stop_event,
            "start_time": time.time(),
            "user_count": len(user_ids)
        })
        
        return task
    
    def stop_all_tasks(self) -> int:
        """
        Stop all running DM tasks.
        
        Returns:
            int: Number of tasks stopped.
        """
        stopped_count = 0
        
        for task_info in self.tasks:
            if not task_info["task"].done():
                task_info["stop_event"].set()
                stopped_count += 1
        
        self.running = False
        logger.info(f"Stopped {stopped_count} DM tasks")
        return stopped_count
    
    def get_running_tasks(self) -> List[Dict[str, Any]]:
        """
        Get information about currently running tasks.
        
        Returns:
            List[Dict[str, Any]]: List of task information dictionaries.
        """
        # Clean up completed tasks
        self.tasks = [t for t in self.tasks if not t["task"].done()]
        
        return [
            {
                "start_time": task["start_time"],
                "duration": time.time() - task["start_time"],
                "user_count": task["user_count"],
                "progress": self.current_progress,
                "success_count": self.success_count,
                "fail_count": self.fail_count,
                "rate_limit_count": self.rate_limit_count
            }
            for task in self.tasks
        ]
    
    def is_running(self) -> bool:
        """
        Check if a bulk DM operation is currently running.
        
        Returns:
            bool: True if an operation is running, False otherwise.
        """
        return self.running