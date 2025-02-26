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

    async def send_dm(self, user_id: str, message: str, token: str) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """Send a direct message to a Discord user using direct API call with enhanced human simulation."""
        
        logger.info(f"Preparing to send DM to user {user_id}")
        
        # 生成更真实的请求头
        chrome_version = random.randint(100, 120)
        build_number = random.randint(180000, 200000)
        random_fingerprint = ''.join(random.choices('0123456789abcdef', k=32))
        
        # 更真实的请求头，包含所有Discord客户端会发送的头信息
        headers = {
            'Authorization': token,
            'Content-Type': 'application/json',
            'User-Agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version}.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://discord.com/channels/@me',
            'Origin': 'https://discord.com',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'Connection': 'keep-alive',
            'DNT': '1',
            'X-Debug-Options': 'bugReporterEnabled',
            'X-Discord-Locale': random.choice(['en-US', 'en-GB', 'fr', 'de', 'pt-BR']),
            'X-Discord-Timezone': random.choice(['America/New_York', 'Europe/London', 'Asia/Tokyo', 'Australia/Sydney']),
            'X-Super-Properties': f'eyJvcyI6IldpbmRvd3MiLCJicm93c2VyIjoiQ2hyb21lIiwiZGV2aWNlIjoiIiwic3lzdGVtX2xvY2FsZSI6ImVuLVVTIiwiYnJvd3Nlcl91c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzExMC4wLjAuMCBTYWZhcmkvNTM3LjM2IiwiYnJvd3Nlcl92ZXJzaW9uIjoiMTEwLjAuMC4wIiwib3NfdmVyc2lvbiI6IjEwIiwicmVmZXJyZXIiOiIiLCJyZWZlcnJpbmdfZG9tYWluIjoiIiwicmVmZXJyZXJfY3VycmVudCI6IiIsInJlZmVycmluZ19kb21haW5fY3VycmVudCI6IiIsInJlbGVhc2VfY2hhbm5lbCI6InN0YWJsZSIsImNsaWVudF9idWlsZF9udW1iZXIiOntidWlsZF9udW1iZXJ9LCJjbGllbnRfZXZlbnRfc291cmNlIjpudWxsfQ==',
            'X-Fingerprint': random_fingerprint,
            'Cookie': f'__dcfduid={random.randint(100000, 999999)}; __sdcfduid={random.randint(100000, 999999)}; locale={random.choice(["en-US", "en-GB", "fr", "de"])}; __cf_bm={random_fingerprint}'
        }
        
        # 获取一个随机会话ID
        session_id = ''.join(random.choices('0123456789abcdef', k=32))
        
        # 在多次请求之间重用一个会话以保持一致性
        async with aiohttp.ClientSession(headers=headers) as session:
            # 实现验证码解决状态追踪
            captcha_already_solved = False
            captcha_solution = None
            
            try:
                # 步骤1: 随机性的第三方服务器调用 - 模拟人类行为链
                # Discord客户端通常会调用多个API端点，我们随机选择一些进行调用
                # 这些调用有助于建立更可信的API调用模式
                
                human_simulation_endpoints = [
                    # 大多数人在发送DM前会先查看一些内容
                    'https://discord.com/api/v9/users/@me/library',
                    'https://discord.com/api/v9/users/@me/settings',
                    'https://discord.com/api/v9/users/@me/guilds',
                    f'https://discord.com/api/v9/users/{user_id}/profile',
                    'https://discord.com/api/v9/experiments'
                ]
                
                # 随机选择1-2个端点访问，看起来更像人类行为
                selected_endpoints = random.sample(human_simulation_endpoints, 
                                                k=min(random.randint(1, 2), 
                                                    len(human_simulation_endpoints)))
                
                for endpoint in selected_endpoints:
                    try:
                        logger.info(f"Simulating human activity by accessing {endpoint}")
                        async with session.get(endpoint) as resp:
                            # 我们不关心响应，只是建立可信的访问模式
                            logger.debug(f"Human simulation call status: {resp.status}")
                        # 短暂延迟，就像人类在不同页面间浏览一样
                        await asyncio.sleep(random.uniform(0.5, 1.2))
                    except Exception as e:
                        logger.debug(f"Error during human simulation (continuing anyway): {str(e)}")
                
                # 步骤2: 创建DM通道
                create_dm_url = 'https://discord.com/api/v9/users/@me/channels'
                create_dm_payload = {
                    'recipient_id': user_id,
                    # 添加额外字段使请求看起来更真实
                    '_trace': [f"discord.{random.randint(1000, 9999)}", f"{session_id}"]
                }
                
                channel_id = None
                channel_data = None
                
                # 验证码处理循环 - 创建DM通道
                max_captcha_retries = 3
                for captcha_retry in range(max_captcha_retries):
                    if captcha_already_solved:
                        headers["X-Captcha-Key"] = captcha_solution
                    
                    try:
                        logger.info(f"Creating DM channel (attempt {captcha_retry+1})")
                        
                        # 添加一些随机参数，看起来更像真实请求
                        request_time = int(time.time() * 1000)
                        
                        # 添加一个随机查询参数以防止缓存
                        query_params = {
                            'timestamp': request_time
                        }
                        
                        async with session.post(
                            create_dm_url, 
                            params=query_params,
                            json=create_dm_payload
                        ) as response:
                            response_status = response.status
                            response_text = await response.text()
                            
                            # 解析响应
                            if response_status == 200 or response_status == 201:
                                try:
                                    channel_data = json.loads(response_text) if response_text else {}
                                    channel_id = channel_data.get('id')
                                    if channel_id:
                                        logger.info(f"Successfully created DM channel: {channel_id}")
                                        break  # 成功创建，跳出循环
                                    else:
                                        logger.error("Channel ID not found in response")
                                        return False, "Could not get DM channel ID", {"error_type": "no_channel_id"}
                                except json.JSONDecodeError:
                                    logger.error(f"JSON parse error for channel data: {response_text}")
                                    return False, "JSON parse error for channel data", {"error_type": "json_error"}
                            else:
                                # 处理验证码或其他错误
                                try:
                                    response_json = json.loads(response_text) if response_text else {}
                                    logger.debug(f"Channel creation error: {response_json}")
                                    
                                    # 检查是否需要验证码
                                    if ("captcha_key" in response_json and 
                                    ("captcha-required" in str(response_json.get("captcha_key", "")) or 
                                        "captcha_required" in str(response_json))):
                                        
                                        # 提取验证码数据
                                        captcha_sitekey = response_json.get("captcha_sitekey")
                                        captcha_rqdata = response_json.get("captcha_rqdata")
                                        captcha_rqtoken = response_json.get("captcha_rqtoken")
                                        captcha_service = response_json.get("captcha_service", "hcaptcha")
                                        
                                        logger.info(f"Captcha required (service: {captcha_service})")
                                        
                                        if captcha_sitekey:
                                            logger.info(f"Attempting to solve captcha with sitekey: {captcha_sitekey}")
                                            captcha_solution = await self.solve_discord_captcha(
                                                captcha_sitekey, captcha_rqdata, captcha_rqtoken
                                            )
                                            
                                            if captcha_solution:
                                                logger.info("Captcha solved successfully")
                                                captcha_already_solved = True
                                                
                                                # 将验证码解决方案添加到请求头
                                                headers["X-Captcha-Key"] = captcha_solution
                                                
                                                # 模拟人类短暂思考
                                                await asyncio.sleep(random.uniform(0.5, 1.5))
                                                
                                                # 继续下一次创建尝试
                                                continue
                                            else:
                                                logger.error("Failed to solve captcha")
                                                return False, "Failed to solve captcha", {"error_type": "captcha_solving_failed"}
                                        else:
                                            logger.error("Captcha required but no sitekey provided")
                                            return False, "Captcha required but missing sitekey", {"error_type": "missing_captcha_data"}
                                    
                                    # 处理其他类型的错误
                                    elif "message" in response_json:
                                        error_message = response_json.get("message", "Unknown error")
                                        logger.error(f"Discord API error: {error_message}")
                                        # 针对特定错误的处理
                                        if "rate limited" in error_message.lower():
                                            retry_after = response_json.get("retry_after", 5)
                                            logger.info(f"Rate limited. Retry after {retry_after}s")
                                            return False, f"Rate limited: {error_message}", {
                                                "error_type": "rate_limited", 
                                                "retry_after": retry_after
                                            }
                                        else:
                                            return False, f"API error: {error_message}", {
                                                "error_type": "api_error",
                                                "message": error_message
                                            }
                                    else:
                                        return False, f"Failed to create DM channel: {response_text}", {
                                            "error_type": "unknown_error", 
                                            "status": response_status
                                        }
                                except json.JSONDecodeError:
                                    logger.error(f"Invalid JSON in error response: {response_text}")
                                    return False, f"Invalid response from Discord API", {
                                        "error_type": "invalid_response", 
                                        "status": response_status
                                    }
                    except Exception as e:
                        logger.error(f"Exception during channel creation: {str(e)}")
                        return False, f"Connection error: {str(e)}", {"error_type": "connection_error"}
                
                # 验证是否成功获取了channel_id
                if not channel_id:
                    logger.error("Failed to create DM channel after multiple attempts")
                    return False, "Failed to create DM channel after retries", {"error_type": "channel_creation_failed"}
                
                # 步骤3: 模拟消息准备和发送
                # 随机等待一小段时间，就像用户在思考消息一样
                thinking_time = random.uniform(0.8, 2.5)
                logger.info(f"Simulating message composition for {thinking_time}s")
                await asyncio.sleep(thinking_time)
                
                # 发送typing指示器
                typing_url = f'https://discord.com/api/v9/channels/{channel_id}/typing'
                
                # 根据消息长度计算适当的打字时间
                message_length = len(message)
                typing_speed = random.uniform(5.0, 12.0)  # 每秒字符数
                typing_time = min(message_length / typing_speed, 5.0)  # 最多5秒
                
                # 分多次发送typing指示器，看起来更自然
                typing_intervals = max(min(int(typing_time / 2) + 1, 3), 1)  # 1-3次
                
                for i in range(typing_intervals):
                    try:
                        async with session.post(typing_url) as typing_resp:
                            logger.debug(f"Typing indicator {i+1}/{typing_intervals}: {typing_resp.status}")
                        
                        interval_time = typing_time / typing_intervals
                        await asyncio.sleep(interval_time)
                    except Exception as e:
                        logger.debug(f"Error sending typing indicator (continuing): {str(e)}")
                
                # 构建消息payload
                message_nonce = str(int(time.time() * 1000)) + str(random.randint(1000, 9999))
                
                message_payload = {
                    'content': message,
                    'nonce': message_nonce,
                    'tts': False,
                    'flags': 0,
                    # 添加一些其他Discord客户端会发送的字段
                    'message_reference': None,
                    'allowed_mentions': {"parse": ["users", "roles", "everyone"], "replied_user": True},
                    'sticker_ids': []
                }
                
                # 发送消息
                send_message_url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
                
                # 验证码处理循环 - 发送消息
                for msg_captcha_retry in range(max_captcha_retries):
                    if captcha_already_solved:
                        headers["X-Captcha-Key"] = captcha_solution
                    
                    try:
                        logger.info(f"Sending message (attempt {msg_captcha_retry+1})")
                        
                        async with session.post(send_message_url, json=message_payload) as msg_response:
                            msg_status = msg_response.status
                            msg_text = await msg_response.text()
                            
                            # 处理成功情况
                            if msg_status == 200 or msg_status == 201:
                                try:
                                    msg_data = json.loads(msg_text) if msg_text else {}
                                    logger.info(f"Message sent successfully: ID {msg_data.get('id', 'unknown')}")
                                    
                                    return True, "Message sent successfully", {
                                        "message_id": msg_data.get('id'),
                                        "channel_id": channel_id,
                                        "timestamp": msg_data.get('timestamp'),
                                        "user_id": user_id
                                    }
                                except json.JSONDecodeError:
                                    # 状态码表示成功但无法解析响应
                                    logger.info("Message sent successfully (no parseable data)")
                                    return True, "Message sent successfully", {
                                        "channel_id": channel_id,
                                        "user_id": user_id
                                    }
                            
                            # 处理错误情况
                            else:
                                try:
                                    error_json = json.loads(msg_text) if msg_text else {}
                                    logger.debug(f"Message send error: {error_json}")
                                    
                                    # 检查是否需要验证码
                                    if ("captcha_key" in error_json and 
                                    ("captcha-required" in str(error_json.get("captcha_key", "")) or 
                                        "captcha_required" in str(error_json))):
                                        
                                        # 验证码数据提取
                                        captcha_sitekey = error_json.get("captcha_sitekey")
                                        captcha_rqdata = error_json.get("captcha_rqdata")
                                        captcha_rqtoken = error_json.get("captcha_rqtoken")
                                        captcha_service = error_json.get("captcha_service", "hcaptcha")
                                        
                                        logger.info(f"Message captcha required (service: {captcha_service})")
                                        
                                        if captcha_sitekey:
                                            logger.info(f"Attempting to solve message captcha: {captcha_sitekey}")
                                            captcha_solution = await self.solve_discord_captcha(
                                                captcha_sitekey, captcha_rqdata, captcha_rqtoken
                                            )
                                            
                                            if captcha_solution:
                                                logger.info("Message captcha solved successfully")
                                                captcha_already_solved = True
                                                
                                                # 添加验证码到头部
                                                headers["X-Captcha-Key"] = captcha_solution
                                                
                                                # 模拟人类短暂思考
                                                await asyncio.sleep(random.uniform(0.5, 1.0))
                                                
                                                # 继续下一次尝试
                                                continue
                                            else:
                                                logger.error("Failed to solve message captcha")
                                                return False, "Failed to solve message captcha", {"error_type": "message_captcha_failed"}
                                        else:
                                            logger.error("Message captcha required but no sitekey provided")
                                            return False, "Message captcha required but missing sitekey", {"error_type": "missing_message_captcha_data"}
                                    
                                    # 处理速率限制
                                    elif "retry_after" in error_json:
                                        retry_after = error_json.get("retry_after", 5)
                                        logger.warning(f"Rate limited. Retry after {retry_after}s")
                                        return False, f"Rate limited when sending message", {
                                            "error_type": "rate_limited",
                                            "retry_after": retry_after
                                        }
                                    
                                    # 处理其他API错误
                                    elif "message" in error_json:
                                        error_message = error_json.get("message", "Unknown error")
                                        logger.error(f"Message send API error: {error_message}")
                                        
                                        # 检查错误原因
                                        if "cannot send messages to this user" in error_message.lower():
                                            return False, "Cannot send messages to this user", {"error_type": "user_blocked_dms"}
                                        else:
                                            return False, f"Message API error: {error_message}", {
                                                "error_type": "message_api_error",
                                                "message": error_message
                                            }
                                    else:
                                        logger.error(f"Unknown message send error: {msg_text}")
                                        return False, f"Failed to send message: Status {msg_status}", {
                                            "error_type": "unknown_message_error", 
                                            "status": msg_status
                                        }
                                except json.JSONDecodeError:
                                    logger.error(f"Invalid JSON in message error response: {msg_text}")
                                    return False, f"Invalid response when sending message", {
                                        "error_type": "invalid_message_response", 
                                        "status": msg_status
                                    }
                    except Exception as e:
                        logger.error(f"Exception during message sending: {str(e)}")
                        return False, f"Message connection error: {str(e)}", {"error_type": "message_connection_error"}
                    
                    # 如果到这里，说明遇到了错误但不是验证码相关的，结束重试
                    break
                    
            except Exception as e:
                error_msg = f"Unexpected error in send_dm: {str(e)}"
                logger.error(error_msg)
                import traceback
                logger.debug(traceback.format_exc())
                return False, error_msg, {"error_type": "unexpected_exception", "error": str(e)}
        
        # 正常情况不应该到达这里，但以防万一
        return False, "Failed to send message after all attempts", {"error_type": "all_attempts_failed"}

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