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

    async def _view_user_profile(self, session, user_id):
        """查看用户个人资料，模拟正常行为"""
        try:
            profile_url = f'https://discord.com/api/v9/users/{user_id}/profile'
            logger.info(f"Simulating human activity by accessing {profile_url}")
            async with session.get(profile_url) as resp:
                logger.debug(f"Profile access status: {resp.status}")
            return True
        except Exception as e:
            logger.debug(f"Error during profile access: {e}")
            return False
            
    async def _check_user_settings(self, session):
        """检查用户设置，模拟正常行为"""
        try:
            settings_url = 'https://discord.com/api/v9/users/@me/settings'
            logger.info(f"Simulating human activity by accessing user settings")
            async with session.get(settings_url) as resp:
                logger.debug(f"Settings access status: {resp.status}")
            return True
        except Exception as e:
            logger.debug(f"Error during settings access: {e}")
            return False
            
    async def _view_user_guilds(self, session):
        """查看用户服务器列表，模拟正常行为"""
        try:
            guilds_url = 'https://discord.com/api/v9/users/@me/guilds'
            logger.info(f"Simulating human activity by accessing guilds")
            async with session.get(guilds_url) as resp:
                logger.debug(f"Guilds access status: {resp.status}")
            return True
        except Exception as e:
            logger.debug(f"Error during guilds access: {e}")
            return False
            
    async def _check_notifications(self, session):
        """检查通知，模拟正常行为"""
        try:
            notif_url = 'https://discord.com/api/v9/users/@me/mentions'
            logger.info(f"Simulating human activity by checking notifications")
            async with session.get(notif_url) as resp:
                logger.debug(f"Notifications access status: {resp.status}")
            return True
        except Exception as e:
            logger.debug(f"Error during notifications check: {e}")
            return False
            
    async def _create_dm_channel(self, session, user_id):
        """创建DM通道"""
        create_dm_url = 'https://discord.com/api/v9/users/@me/channels'
        create_dm_payload = {'recipient_id': user_id}
        
        try:
            logger.info("Creating DM channel (attempt 1)")
            async with session.post(create_dm_url, json=create_dm_payload) as response:
                if response.status == 200 or response.status == 201:
                    response_text = await response.text()
                    try:
                        channel_data = json.loads(response_text)
                        channel_id = channel_data.get('id')
                        if channel_id:
                            logger.info(f"Successfully created DM channel: {channel_id}")
                            return channel_data, channel_id
                    except json.JSONDecodeError:
                        pass
                        
                return None, None
        except Exception:
            return None, None
            
    async def _send_typing_indicator(self, session, channel_id):
        """发送正在输入指示器"""
        try:
            typing_url = f'https://discord.com/api/v9/channels/{channel_id}/typing'
            async with session.post(typing_url) as typing_resp:
                return typing_resp.status == 200 or typing_resp.status == 204
        except Exception:
            return False
    
    async def send_dm(self, user_id: str, message: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Send a direct message to a Discord user with enhanced anti-detection and captcha handling."""
        
        logger.info(f"Preparing to send DM to user {user_id}")
        
        # 生成随机的请求标识符
        chrome_version = random.randint(100, 120)
        build_number = random.randint(180000, 200000)
        
        # 尝试从缓存中检索验证码解决方案
        captcha_solution = self.token_manager.get_cached_captcha_solution(token) if hasattr(self, 'token_manager') else None
        
        # 更真实的请求头，模拟正常的Discord客户端
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json',
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version}.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://discord.com/channels/@me',
            'Origin': 'https://discord.com',
            'X-Super-Properties': f'eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiQ2hyb21lIiwiZGV2aWNlIjoiIiwic3lzdGVtX2xvY2FsZSI6ImVuLVVTIiwiYnJvd3Nlcl91c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzExMC4wLjAuMCBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTEwLjAuMC4wIiwib3NfdmVyc2lvbiI6IjEwIiwicmVmZXJyZXIiOiIiLCJyZWZlcnJpbmdfZG9tYWluIjoiIiwicmVmZXJyZXJfY3VycmVudCI6IiIsInJlZmVycmluZ19kb21haW5fY3VycmVudCI6IiIsInJlbGVhc2VfY2hhbm5lbCI6InN0YWJsZSIsImNsaWVudF9idWlsZF9udW1iZXIiOntidWlsZF9udW1iZXJ9LCJjbGllbnRfZXZlbnRfc291cmNlIjpudWxsfQ==',
        }
        
        # 如果我们有缓存的验证码解决方案，添加到请求头
        if captcha_solution:
            headers["X-Captcha-Key"] = captcha_solution
            logger.info("Using cached captcha solution")
        
        try:
            # 每个帐户的Cookie应该是唯一的
            cookies = {
                'locale': 'en-US',
                '__dcfduid': f'{random.randint(100000, 999999)}',
                '__sdcfduid': f'{random.randint(100000, 999999)}'
            }
            
            async with aiohttp.ClientSession(headers=headers, cookies=cookies) as session:
                # 第1步: 降低检测风险 - 执行一系列正常的Discord操作
                # 这模拟了用户在发送DM前的正常浏览行为
                
                # 根据随机选择执行1-2个常规操作
                normal_operations = [
                    lambda: self._view_user_profile(session, user_id),
                    lambda: self._check_user_settings(session),
                    lambda: self._view_user_guilds(session),
                    lambda: self._check_notifications(session)
                ]
                
                # 随机选择1-2个操作执行
                selected_ops = random.sample(normal_operations, k=min(2, len(normal_operations)))
                for op in selected_ops:
                    await op()
                    # 在操作之间等待一会儿
                    await asyncio.sleep(random.uniform(0.5, 1.0))
                
                # 第2步: 创建DM通道
                channel_data, channel_id = await self._create_dm_channel(session, user_id)
                if not channel_id:
                    return False, "Failed to create DM channel", {"error_type": "channel_creation_failed"}
                
                # 第3步: 发送消息，处理验证码
                # 添加强化的验证码跟踪
                captcha_retry_count = 0
                captcha_solutions_tried = set()  # 跟踪已尝试的验证码解决方案
                max_captcha_retries = 3  # 最多尝试3次验证码
                
                # 模拟消息撰写
                await asyncio.sleep(random.uniform(0.8, 1.5))
                
                # 发送typing指示器
                typing_success = await self._send_typing_indicator(session, channel_id)
                
                # 第4步: 构建消息payload
                message_nonce = str(int(time.time() * 1000))
                message_payload = {
                    'content': message,
                    'nonce': message_nonce,
                    'tts': False
                }
                
                # 解决验证码循环检测
                same_captcha_count = 0  # 跟踪同一个验证码出现的次数
                last_captcha_sitekey = None  # 上一个验证码的sitekey
                
                # 发送消息尝试循环
                for attempt in range(5):  # 最多尝试5次
                    send_message_url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
                    
                    try:
                        logger.info(f"Sending message (attempt {attempt+1})")
                        
                        # 如果有缓存的验证码，添加到头部
                        if captcha_solution and "X-Captcha-Key" not in headers:
                            headers["X-Captcha-Key"] = captcha_solution
                        
                        async with session.post(send_message_url, json=message_payload) as response:
                            status = response.status
                            response_text = await response.text()
                            
                            # 成功发送
                            if status == 200 or status == 201:
                                try:
                                    message_data = json.loads(response_text)
                                    logger.info(f"Message sent successfully: ID {message_data.get('id', 'unknown')}")
                                    
                                    # 缓存成功的验证码解决方案，如果它帮助我们通过了
                                    if captcha_solution and hasattr(self, 'token_manager'):
                                        self.token_manager.cache_captcha_solution(token, captcha_solution)
                                    
                                    return True, "Message sent successfully", {
                                        "message_id": message_data.get('id'),
                                        "channel_id": channel_id,
                                        "timestamp": message_data.get('timestamp')
                                    }
                                except json.JSONDecodeError:
                                    # JSON解析失败但状态码是成功的
                                    logger.info("Message sent successfully (no parseable data)")
                                    return True, "Message sent successfully", {
                                        "channel_id": channel_id
                                    }
                            
                            # 处理需要验证码的情况
                            try:
                                response_json = json.loads(response_text)
                                
                                # 检查是否需要验证码
                                if "captcha_key" in response_json or "captcha_sitekey" in response_json:
                                    captcha_sitekey = response_json.get("captcha_sitekey")
                                    captcha_rqdata = response_json.get("captcha_rqdata")
                                    
                                    # 检测无限验证码循环
                                    if captcha_sitekey == last_captcha_sitekey:
                                        same_captcha_count += 1
                                        logger.warning(f"Same captcha detected {same_captcha_count} times in a row")
                                        
                                        if same_captcha_count >= 2:
                                            logger.error("Detected infinite captcha loop - Discord may have flagged this token")
                                            # 尝试最终的解决方案 - 延迟后带上所有可能的头部再试一次
                                            await asyncio.sleep(5)  # 等待5秒
                                            
                                            # 增强头部，尝试击败验证码循环
                                            headers["X-Captcha-Key"] = captcha_solution
                                            headers["X-Discord-Locale"] = "en-US" 
                                            headers["X-Debug-Options"] = "bugReporterEnabled"
                                            headers["Origin"] = "https://discord.com"
                                            headers["Alt-Used"] = "discord.com"
                                            headers["Connection"] = "keep-alive"
                                            
                                            # 最后的尝试
                                            async with session.post(send_message_url, json=message_payload, headers=headers) as final_attempt:
                                                if final_attempt.status == 200 or final_attempt.status == 201:
                                                    logger.info("Successfully broke out of captcha loop!")
                                                    return True, "Message sent successfully after breaking captcha loop", {
                                                        "channel_id": channel_id
                                                    }
                                            
                                            return False, "Infinite captcha loop detected", {
                                                "error_type": "infinite_captcha_loop",
                                                "token_status": "flagged"
                                            }
                                    else:
                                        last_captcha_sitekey = captcha_sitekey
                                        same_captcha_count = 1
                                    
                                    captcha_retry_count += 1
                                    logger.info(f"Message captcha required ({captcha_retry_count}/{max_captcha_retries})")
                                    
                                    # 检查是否达到最大重试次数
                                    if captcha_retry_count > max_captcha_retries:
                                        logger.error(f"Exceeded maximum captcha retry limit ({max_captcha_retries})")
                                        return False, "Too many captchas required", {
                                            "error_type": "excessive_captchas",
                                            "token_status": "check"
                                        }
                                    
                                    # 尝试解决验证码
                                    captcha_solution = await self.solve_discord_captcha(
                                        captcha_sitekey, captcha_rqdata, None
                                    )
                                    
                                    if captcha_solution:
                                        # 记录此解决方案已尝试过
                                        captcha_solutions_tried.add(captcha_solution)
                                        
                                        # 添加验证码到头部
                                        headers["X-Captcha-Key"] = captcha_solution
                                        
                                        # 随机等待一下再尝试
                                        await asyncio.sleep(random.uniform(0.5, 1.0))
                                        continue
                                    else:
                                        logger.error("Failed to solve captcha")
                                        return False, "Failed to solve captcha", {"error_type": "captcha_solving_failed"}
                                
                                # 处理速率限制
                                elif "retry_after" in response_json:
                                    retry_after = response_json.get("retry_after", 5)
                                    logger.warning(f"Rate limited. Retry after {retry_after}s")
                                    return False, f"Rate limited", {
                                        "error_type": "rate_limited",
                                        "retry_after": retry_after
                                    }
                                
                                # 其他API错误
                                elif "message" in response_json:
                                    error_message = response_json.get("message", "Unknown error")
                                    logger.error(f"Discord API error: {error_message}")
                                    return False, f"API error: {error_message}", {
                                        "error_type": "api_error",
                                        "message": error_message
                                    }
                                
                            except json.JSONDecodeError:
                                logger.error(f"Invalid JSON in response: {response_text}")
                                return False, "Invalid response from Discord API", {
                                    "error_type": "invalid_response", 
                                    "status": status
                                }
                            
                    except Exception as e:
                        logger.error(f"Exception during message sending: {str(e)}")
                        return False, f"Error sending message: {str(e)}", {
                            "error_type": "exception", 
                            "error": str(e)
                        }
                    
                    # 短暂等待后再尝试
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                
                # 如果所有尝试都失败了
                return False, "Failed to send message after multiple attempts", {
                    "error_type": "max_attempts_reached"
                }
                
        except Exception as e:
            error_msg = f"Unexpected error in send_dm: {str(e)}"
            logger.error(error_msg)
            import traceback
            logger.debug(traceback.format_exc())
            return False, error_msg, {"error_type": "unexpected_exception"}

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
                
                # 只设置rqdata，不设置rqtoken (根据错误信息，anti-captcha不支持rqtoken)
                if rqdata:
                    logger.info(f"Setting enterprise payload with rqdata")
                    solver.set_enterprise_payload({
                        "rqdata": rqdata
                    })
                
                # 解决验证码
                logger.info("Sending captcha to anti-captcha service")
                captcha_key = solver.solve_and_return_solution()
                
                if captcha_key != 0:
                    logger.info("Captcha successfully solved")
                    return captcha_key
                else:
                    logger.error(f"Error solving captcha: {solver.error_code}")
                    return None
                    
            except Exception as e:
                logger.error(f"Exception during captcha solving: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                return None
        
        # 添加重试逻辑
        max_captcha_retries = 2
        for retry in range(max_captcha_retries):
            try:
                # 在事件循环的执行器中运行同步任务
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(None, solve_captcha_sync)
                
                if result:
                    return result
                
                logger.warning(f"Captcha solving failed, retry {retry+1}/{max_captcha_retries}")
                await asyncio.sleep(2)  # 短暂等待后重试
                
            except Exception as e:
                logger.error(f"Error in solve_discord_captcha: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                
                # 最后一次重试失败
                if retry == max_captcha_retries - 1:
                    logger.error("All captcha solving attempts failed")
                    return None
                
                await asyncio.sleep(2)  # 短暂等待后重试
        
        return None

    async def pre_authenticate_captcha(self, token):
        """
        预先验证验证码解决方案
        
        有时Discord会连续要求多个验证码。这个方法尝试预先解决验证码，
        这样我们就有一个准备好的解决方案，可以重复使用
        
        Args:
            token (str): Discord令牌
                
        Returns:
            tuple: (成功标志, 验证码解决方案, 错误消息)
        """
        logger.info("Attempting to pre-authenticate captcha")
        
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json',
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{random.randint(100, 115)}.0.0.0 Safari/537.36',
        }
        
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                # 访问一个需要验证的端点
                async with session.get('https://discord.com/api/v9/users/@me/settings') as response:
                    if response.status == 200:
                        logger.info("No captcha needed for pre-authentication")
                        return True, None, "No captcha needed"
                    
                    # 检查是否需要验证码
                    response_text = await response.text()
                    try:
                        response_json = json.loads(response_text)
                        if "captcha_key" in response_json:
                            # 提取验证码数据
                            captcha_sitekey = response_json.get("captcha_sitekey")
                            captcha_rqdata = response_json.get("captcha_rqdata")
                            captcha_rqtoken = response_json.get("captcha_rqtoken")
                            
                            logger.info(f"Captcha required during pre-authentication, sitekey: {captcha_sitekey}")
                            
                            # 解决验证码
                            captcha_key = await self.solve_discord_captcha(
                                captcha_sitekey, captcha_rqdata, captcha_rqtoken
                            )
                            
                            if captcha_key:
                                logger.info("Pre-authentication captcha solved successfully")
                                
                                # 尝试验证解决方案
                                headers["X-Captcha-Key"] = captcha_key
                                
                                # 重新尝试请求
                                async with session.get('https://discord.com/api/v9/users/@me/settings', 
                                                    headers=headers) as verify_resp:
                                    if verify_resp.status == 200:
                                        logger.info("Captcha solution successfully verified")
                                        return True, captcha_key, "Captcha solved and verified"
                                    else:
                                        logger.warning("Captcha solution failed verification")
                                        return False, None, "Captcha solution verification failed"
                            else:
                                logger.error("Failed to solve pre-authentication captcha")
                                return False, None, "Failed to solve captcha"
                        else:
                            # 其他错误
                            error_message = response_json.get("message", "Unknown error")
                            logger.warning(f"Pre-authentication error: {error_message}")
                            return False, None, f"API error: {error_message}"
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON in pre-authentication response: {response_text}")
                        return False, None, "Invalid JSON response"
        except Exception as e:
            logger.error(f"Exception during pre-authentication: {e}")
            return False, None, f"Connection error: {str(e)}"

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