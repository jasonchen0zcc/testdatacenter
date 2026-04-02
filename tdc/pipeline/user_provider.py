from typing import List, Optional, Union

import httpx

from tdc.config.models import ExecutionConfig, UserHttpConfig
from tdc.core.exceptions import UserSourceError
from tdc.pipeline.context import ContextManager


class UserProvider:
    """用户来源提供者"""

    def __init__(self, config: ExecutionConfig, context_manager: ContextManager):
        self.config = config
        self.context_manager = context_manager
        self._users: List[dict] = []

    def initialize(self) -> None:
        """初始化用户列表"""
        if self.config.user_source == "faker":
            # faker 模式：延迟生成，无需初始化
            pass
        elif self.config.user_source == "http":
            http_config = self.config.user_http
            if http_config and http_config.single_user:
                # 单用户模式：每次迭代实时获取，无需预取
                pass
            else:
                # 批量获取模式：支持返回列表或单个对象
                result = self._fetch_users_from_http()
                if isinstance(result, list):
                    self._users = result
                elif isinstance(result, dict):
                    # 单个对象，包装成列表
                    self._users = [result]
                else:
                    raise UserSourceError(f"Unexpected user data type: {type(result)}")
        elif self.config.user_source == "list":
            self._users = self.config.user_list or []
        else:
            raise UserSourceError(f"Unknown user_source: {self.config.user_source}")

    def get_user(self, iteration: int) -> Union[str, dict]:
        """获取第 iteration 个用户"""
        if self.config.user_source == "faker":
            # 每次渲染 template 生成新用户
            return self.context_manager.render_template(
                self.config.user_template or "{{ faker.username }}"
            )
        elif self.config.user_source == "http":
            http_config = self.config.user_http
            if http_config and http_config.single_user:
                # 单用户模式：每次实时调用接口获取新用户
                user = self._fetch_single_user()
                return user
            # 从预获取的列表中按序取用
            if not self._users:
                raise UserSourceError(f"No users available from source: {self.config.user_source}")
            # 循环使用：iteration % len(users)
            return self._users[iteration % len(self._users)]
        elif self.config.user_source == "list":
            # 从预获取的列表中按序取用
            if not self._users:
                raise UserSourceError(f"No users available from source: {self.config.user_source}")
            return self._users[iteration % len(self._users)]
        else:
            raise UserSourceError(f"Unknown user_source: {self.config.user_source}")

    def _fetch_users_from_http(self) -> Union[List[dict], dict]:
        """从 HTTP 接口获取用户列表或单个用户"""
        http_config = self.config.user_http
        if not http_config:
            raise UserSourceError("user_http config required for http source")

        try:
            # 准备请求参数
            request_kwargs = {
                "method": http_config.method,
                "url": http_config.url,
                "headers": http_config.headers,
                "timeout": 30
            }
            # POST 请求添加 body
            if http_config.method.upper() == "POST" and http_config.body:
                request_kwargs["content"] = http_config.body

            # 发送 HTTP 请求
            response = httpx.request(**request_kwargs)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            raise UserSourceError(f"Failed to fetch users from HTTP: {e}")
        except Exception as e:
            raise UserSourceError(f"Failed to parse users response: {e}")

        # JSONPath 提取用户数据（可能是列表或单个对象）
        users = self._extract_by_path(data, http_config.user_path)
        if not isinstance(users, (list, dict)):
            raise UserSourceError(f"Expected list or dict from user_path, got {type(users)}")

        # 如果配置了 user_field，从对象中提取字段
        if http_config.user_field:
            try:
                if isinstance(users, list):
                    users = [
                        self._extract_by_path(user, http_config.user_field)
                        for user in users
                    ]
                else:
                    # 单个对象
                    users = self._extract_by_path(users, http_config.user_field)
            except (KeyError, TypeError) as e:
                raise UserSourceError(f"Failed to extract user_field: {e}")

        return users

    def _fetch_single_user(self) -> dict:
        """从 HTTP 接口获取单个用户"""
        http_config = self.config.user_http
        if not http_config:
            raise UserSourceError("user_http config required for http source")

        try:
            # 准备请求参数
            request_kwargs = {
                "method": http_config.method,
                "url": http_config.url,
                "headers": http_config.headers,
                "timeout": 30
            }
            # POST 请求添加 body
            if http_config.method.upper() == "POST" and http_config.body:
                request_kwargs["content"] = http_config.body

            # 发送 HTTP 请求
            response = httpx.request(**request_kwargs)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            raise UserSourceError(f"Failed to fetch user from HTTP: {e}")
        except Exception as e:
            raise UserSourceError(f"Failed to parse user response: {e}")

        # JSONPath 提取用户对象
        user = self._extract_by_path(data, http_config.user_path)
        if not isinstance(user, dict):
            raise UserSourceError(f"Expected dict from user_path, got {type(user)}")

        return user

    def _extract_by_path(self, data: dict, path: str):
        """使用点号路径从字典提取值"""
        parts = path.split(".")
        current = data
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                raise UserSourceError(f"Path '{path}' not found in data")
        return current
