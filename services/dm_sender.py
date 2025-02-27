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
        """Send a direct message to a Discord user with enhanced captcha handling using BrightData."""
        
        logger.info(f"Preparing to send DM to user {user_id}")
        
        # 生成随机的请求标识符
        chrome_version = random.randint(100, 120)
        build_number = random.randint(180000, 200000)
        
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
        
        # 创建唯一的cookie
        cookies = {
            'locale': 'en-US',
            '__dcfduid': f'{random.randint(100000, 999999)}',
            '__sdcfduid': f'{random.randint(100000, 999999)}'
        }
        
        # 验证码解决方案缓存
        captcha_solutions = {}
        
        try:
            async with aiohttp.ClientSession(headers=headers, cookies=cookies) as session:
                # 步骤1: 模拟正常用户行为
                try:
                    logger.info(f"Simulating human behavior: viewing user profile")
                    profile_url = f'https://discord.com/api/v9/users/{user_id}/profile'
                    async with session.get(profile_url) as resp:
                        logger.debug(f"Profile view status: {resp.status}")
                    await asyncio.sleep(random.uniform(0.5, 1.2))
                except Exception as e:
                    logger.debug(f"Error viewing profile (continuing): {e}")
                
                # 步骤2: 创建DM通道
                create_dm_url = 'https://discord.com/api/v9/users/@me/channels'
                create_dm_payload = {'recipient_id': user_id}
                
                logger.info("Creating DM channel")
                channel_id = None
                
                # 尝试创建DM通道，处理可能的验证码
                for channel_attempt in range(3):  # 最多尝试3次
                    try:
                        async with session.post(create_dm_url, json=create_dm_payload) as response:
                            response_status = response.status
                            response_text = await response.text()
                            
                            if response_status == 200 or response_status == 201:
                                # 成功创建通道
                                channel_data = json.loads(response_text)
                                channel_id = channel_data.get('id')
                                if channel_id:
                                    logger.info(f"Successfully created DM channel: {channel_id}")
                                    break
                                else:
                                    logger.error("Could not get channel ID from response")
                                    return False, "Could not get DM channel ID", {"error_type": "no_channel_id"}
                            else:
                                # 处理可能的验证码
                                try:
                                    response_json = json.loads(response_text)
                                    
                                    # 检查是否需要验证码
                                    if "captcha_key" in response_json or "captcha_sitekey" in response_json:
                                        captcha_sitekey = response_json.get("captcha_sitekey")
                                        captcha_rqdata = response_json.get("captcha_rqdata")
                                        
                                        logger.info(f"Channel creation requires captcha: {captcha_sitekey}")
                                        
                                        # 尝试解决验证码
                                        captcha_solution = await self.solve_discord_captcha(
                                            captcha_sitekey, captcha_rqdata, None
                                        )
                                        
                                        if not captcha_solution:
                                            logger.error("Failed to solve channel captcha")
                                            if channel_attempt == 2:  # 最后一次尝试
                                                return False, "Failed to solve channel captcha", {"error_type": "channel_captcha_failed"}
                                            continue
                                        
                                        # 缓存验证码解决方案
                                        captcha_solutions[captcha_sitekey] = captcha_solution
                                        
                                        # 添加验证码到请求头
                                        headers["X-Captcha-Key"] = captcha_solution
                                        
                                        logger.info("Retrying channel creation with captcha solution")
                                        continue  # 继续循环使用新的验证码
                                    else:
                                        # 其他错误
                                        error_message = response_json.get("message", "Unknown error")
                                        logger.error(f"Channel creation error: {error_message}")
                                        
                                        if channel_attempt == 2:  # 最后一次尝试
                                            return False, f"Failed to create DM channel: {error_message}", {"error_type": "channel_creation_failed"}
                                except json.JSONDecodeError:
                                    logger.error(f"Invalid JSON in channel response: {response_text}")
                                    if channel_attempt == 2:  # 最后一次尝试
                                        return False, "Invalid response from Discord API", {"error_type": "invalid_response"}
                    except Exception as e:
                        logger.error(f"Exception during channel creation: {str(e)}")
                        if channel_attempt == 2:  # 最后一次尝试
                            return False, f"Error creating DM channel: {str(e)}", {"error_type": "exception"}
                    
                    # 短暂等待后重试
                    await asyncio.sleep(1)
                
                # 验证我们有有效的channel_id
                if not channel_id:
                    return False, "Failed to create DM channel after multiple attempts", {"error_type": "channel_creation_failed"}
                
                # 步骤3: 模拟消息撰写
                compose_time = random.uniform(0.8, 2.0)
                logger.info(f"Simulating message composition for {compose_time:.2f}s")
                await asyncio.sleep(compose_time)
                
                # 发送"正在输入"指示器
                try:
                    typing_url = f'https://discord.com/api/v9/channels/{channel_id}/typing'
                    async with session.post(typing_url) as typing_resp:
                        logger.info(f"Sent typing indicator: {typing_resp.status}")
                    
                    # 模拟打字时间，更短以加快速度
                    typing_time = min(len(message) / 15.0, 3.0)  # 加快打字速度
                    await asyncio.sleep(typing_time)
                except Exception as e:
                    logger.warning(f"Error sending typing indicator (continuing): {e}")
                
                # 步骤4: 发送消息
                message_nonce = str(int(time.time() * 1000))
                message_payload = {
                    'content': message,
                    'nonce': message_nonce,
                    'tts': False
                }
                
                send_message_url = f'https://discord.com/api/v9/channels/{channel_id}/messages'
                
                # 验证码循环处理变量
                max_captcha_retries = 3
                current_captcha_retry = 0
                last_captcha_sitekey = None
                same_captcha_count = 0
                
                # 尝试发送消息，处理可能的验证码
                for msg_attempt in range(5):  # 最多尝试5次
                    try:
                        logger.info(f"Sending message (attempt {msg_attempt+1}/5)")
                        
                        async with session.post(send_message_url, json=message_payload) as msg_response:
                            msg_status = msg_response.status
                            msg_text = await msg_response.text()
                            
                            # 检查是否成功
                            if msg_status == 200 or msg_status == 201:
                                try:
                                    message_data = json.loads(msg_text)
                                    logger.info(f"Message sent successfully: ID {message_data.get('id', 'unknown')}")
                                    return True, "Message sent successfully", {
                                        "message_id": message_data.get('id', 'unknown'),
                                        "channel_id": channel_id,
                                        "timestamp": message_data.get('timestamp')
                                    }
                                except:
                                    logger.info("Message sent successfully (no parseable data)")
                                    return True, "Message sent successfully", {"channel_id": channel_id}
                            
                            # 处理错误
                            try:
                                error_json = json.loads(msg_text)
                                
                                # 检查是否需要验证码
                                if "captcha_key" in error_json or "captcha_sitekey" in error_json:
                                    captcha_sitekey = error_json.get("captcha_sitekey")
                                    captcha_rqdata = error_json.get("captcha_rqdata")
                                    
                                    # 检测是否是同一个验证码重复出现（无限循环）
                                    if captcha_sitekey == last_captcha_sitekey:
                                        same_captcha_count += 1
                                        logger.warning(f"Same captcha detected {same_captcha_count} times")
                                        
                                        if same_captcha_count >= 2:
                                            # 可能是无限验证码循环
                                            logger.error("Possible infinite captcha loop detected")
                                            
                                            # 最后的尝试：添加额外头部
                                            headers["X-Discord-Locale"] = "en-US"
                                            headers["X-Debug-Options"] = "bugReporterEnabled"
                                            
                                            if msg_attempt >= 3:  # 如果已经尝试了很多次
                                                return False, "Infinite captcha loop detected", {
                                                    "error_type": "infinite_captcha_loop",
                                                    "token_status": "flagged"
                                                }
                                    else:
                                        # 不同验证码，重置计数器
                                        same_captcha_count = 0
                                        last_captcha_sitekey = captcha_sitekey
                                    
                                    # 增加验证码重试计数
                                    current_captcha_retry += 1
                                    logger.info(f"Message requires captcha ({current_captcha_retry}/{max_captcha_retries}): {captcha_sitekey}")
                                    
                                    # 检查是否超过最大重试次数
                                    if current_captcha_retry > max_captcha_retries:
                                        logger.error(f"Exceeded maximum captcha retries ({max_captcha_retries})")
                                        return False, "Too many captchas required", {
                                            "error_type": "excessive_captchas",
                                            "token_status": "check"
                                        }
                                    
                                    # 检查是否已经解决过这个验证码
                                    if captcha_sitekey in captcha_solutions:
                                        logger.info("Using previously solved captcha")
                                        captcha_solution = captcha_solutions[captcha_sitekey]
                                    else:
                                        # 解决新验证码
                                        captcha_solution = await self.solve_discord_captcha(
                                            captcha_sitekey, captcha_rqdata, None
                                        )
                                        
                                        if not captcha_solution:
                                            if msg_attempt == 4:  # 最后一次尝试
                                                return False, "Failed to solve message captcha", {"error_type": "message_captcha_failed"}
                                            continue
                                        
                                        # 缓存解决方案
                                        captcha_solutions[captcha_sitekey] = captcha_solution
                                    
                                    # 添加验证码到头
                                    headers["X-Captcha-Key"] = captcha_solution
                                    
                                    # 继续下一次尝试
                                    await asyncio.sleep(random.uniform(0.5, 1.0))
                                    continue
                                
                                # 处理速率限制
                                elif "retry_after" in error_json:
                                    retry_after = error_json.get("retry_after", 5)
                                    logger.warning(f"Rate limited. Retry after {retry_after}s")
                                    return False, f"Rate limited", {
                                        "error_type": "rate_limited",
                                        "retry_after": retry_after
                                    }
                                
                                # 其他API错误
                                elif "message" in error_json:
                                    error_message = error_json.get("message", "Unknown error")
                                    logger.error(f"Discord API error: {error_message}")
                                    
                                    # 特殊错误处理
                                    if "cannot send messages to this user" in error_message.lower():
                                        return False, "Cannot send messages to this user", {
                                            "error_type": "user_blocked_dms"
                                        }
                                    
                                    if msg_attempt == 4:  # 最后一次尝试
                                        return False, f"API error: {error_message}", {
                                            "error_type": "api_error",
                                            "message": error_message
                                        }
                                
                            except json.JSONDecodeError:
                                logger.error(f"Invalid JSON in message response: {msg_text}")
                                if msg_attempt == 4:  # 最后一次尝试
                                    return False, f"Invalid response: {msg_status}", {
                                        "error_type": "invalid_response",
                                        "status": msg_status
                                    }
                    
                    except Exception as e:
                        logger.error(f"Exception during message sending: {str(e)}")
                        if msg_attempt == 4:  # 最后一次尝试
                            return False, f"Error sending message: {str(e)}", {
                                "error_type": "exception",
                                "error": str(e)
                            }
                    
                    # 短暂等待后重试
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                
                # 如果所有尝试都失败
                return False, "Failed to send message after all attempts", {
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
        使用BrightData API解决Discord的hCaptcha验证码
        
        Args:
            sitekey (str): hCaptcha的站点密钥
            rqdata (str): 验证码请求数据
            rqtoken (str): 验证码请求令牌（不使用）
                
        Returns:
            str: 解决的验证码密钥或None如果失败
        """
        logger.info(f"Attempting to solve Discord hCaptcha using BrightData API")
        logger.info(f"Sitekey: {sitekey}")
        
        # BrightData API 配置
        api_key = "e596f2c8b85e862fc96b42c5ac784783bbe6f62d462e2400aa946fa9ed337fbe"
        api_endpoint = "https://api.brightdata.com/request"
        
        # 设置认证头
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        # 根据错误信息重新构建请求payload
        # 不要使用captcha作为根级别字段
        captcha_payload = {
            "zone": "web_unlocker1",
            "url": f"https://discord.com/api/v9/hcaptcha?sitekey={sitekey}",
            "hcaptcha": {
                "sitekey": sitekey,
                "url": "https://discord.com"
            },
            "format": "json"
        }
        
        # 如果有rqdata，以查询参数形式添加
        if rqdata:
            captcha_payload["url"] += f"&data={rqdata}"
        
        try:
            async with aiohttp.ClientSession() as session:
                logger.info("Submitting captcha to BrightData API with updated format")
                logger.debug(f"Payload: {json.dumps(captcha_payload)}")
                
                # 发送验证码请求
                async with session.post(api_endpoint, json=captcha_payload, headers=headers) as response:
                    response_status = response.status
                    response_text = await response.text()
                    
                    if response_status != 200:
                        logger.error(f"BrightData API error: {response_status} - {response_text}")
                        return None
                    
                    logger.debug(f"BrightData raw response: {response_text}")
                    
                    # 尝试解析响应
                    try:
                        # 检查是否是JSON
                        try:
                            response_json = json.loads(response_text)
                            logger.debug(f"Parsed response JSON: {response_json}")
                            
                            # BrightData可能返回多种格式的响应，尝试查找验证码令牌
                            
                            # 方式1: 尝试查找response字段中的令牌
                            if "response" in response_json:
                                if isinstance(response_json["response"], dict) and "token" in response_json["response"]:
                                    token = response_json["response"]["token"]
                                    logger.info("Found token in response.token")
                                    return token
                                elif isinstance(response_json["response"], str) and len(response_json["response"]) > 20:
                                    logger.info("Found token in response as string")
                                    return response_json["response"]
                            
                            # 方式2: 查找h_captcha_response字段
                            if "h_captcha_response" in response_json:
                                logger.info("Found token in h_captcha_response")
                                return response_json["h_captcha_response"]
                            
                            # 方式3: 查找solution字段
                            if "solution" in response_json:
                                solution = response_json["solution"]
                                if isinstance(solution, dict):
                                    if "token" in solution:
                                        logger.info("Found token in solution.token")
                                        return solution["token"]
                                    elif "h_captcha_response" in solution:
                                        logger.info("Found token in solution.h_captcha_response")
                                        return solution["h_captcha_response"]
                                elif isinstance(solution, str) and len(solution) > 20:
                                    logger.info("Found token in solution as string")
                                    return solution
                            
                            # 方式4: 根据返回的HTML分析可能包含的令牌
                            if "body" in response_json and isinstance(response_json["body"], str):
                                html_body = response_json["body"]
                                # 尝试从HTML中提取hcaptcha响应
                                import re
                                token_match = re.search(r'name="h-captcha-response" value="([^"]+)"', html_body)
                                if token_match:
                                    logger.info("Found token in HTML body")
                                    return token_match.group(1)
                            
                            # 未找到任何已知格式的令牌
                            logger.error(f"No captcha token found in response: {response_json}")
                            return None
                            
                        except json.JSONDecodeError:
                            # 可能直接返回了令牌字符串
                            if response_text and len(response_text) > 20 and not response_text.startswith("<"):
                                logger.info("BrightData API returned raw token")
                                return response_text.strip()
                            else:
                                logger.error(f"Failed to parse BrightData response: {response_text}")
                                return None
                    except Exception as e:
                        logger.error(f"Error processing BrightData response: {str(e)}")
                        import traceback
                        logger.debug(traceback.format_exc())
                        return None
                    
        except Exception as e:
            logger.error(f"Exception during BrightData captcha solving: {str(e)}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
        
        return None  # 默认返回None
        
    async def _poll_brightdata_task(self, task_id, headers):
        """轮询BrightData任务状态，获取验证码解决方案"""
        api_endpoint = f"https://api.brightdata.com/request/{task_id}"
        max_attempts = 30
        
        for attempt in range(max_attempts):
            # 等待时间逐渐增加
            wait_time = min(2 + attempt * 0.5, 10)
            logger.info(f"Waiting {wait_time}s before checking task status (attempt {attempt+1}/{max_attempts})")
            await asyncio.sleep(wait_time)
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(api_endpoint, headers=headers) as response:
                        if response.status != 200:
                            logger.warning(f"Error checking task status: {response.status}")
                            continue
                        
                        response_text = await response.text()
                        try:
                            response_json = json.loads(response_text)
                            
                            # 检查是否完成
                            status = response_json.get("status")
                            
                            if status == "done":
                                if "solution" in response_json and "captcha" in response_json["solution"]:
                                    solution = response_json["solution"]["captcha"]["token"]
                                    logger.info("Captcha successfully solved")
                                    return solution
                                elif "captcha" in response_json and "token" in response_json["captcha"]:
                                    solution = response_json["captcha"]["token"]
                                    logger.info("Captcha successfully solved")
                                    return solution
                                else:
                                    logger.error(f"No solution in completed task: {response_json}")
                                    return None
                            
                            elif status == "error":
                                error_message = response_json.get("error", "Unknown error")
                                logger.error(f"Task failed: {error_message}")
                                return None
                            
                            elif status in ["pending", "processing"]:
                                logger.info(f"Task is {status}...")
                                continue
                            
                            else:
                                logger.warning(f"Unknown task status: {status}")
                                continue
                            
                        except json.JSONDecodeError:
                            logger.error(f"Failed to parse task status response: {response_text}")
                            continue
                        
            except Exception as e:
                logger.error(f"Error polling task status: {str(e)}")
                continue
        
        logger.error(f"Task polling timed out after {max_attempts} attempts")
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