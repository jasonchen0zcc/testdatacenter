from typing import List, Optional

import httpx

from tdc.config.models import ExecutionConfig, UserHttpConfig
from tdc.core.exceptions import UserSourceError
from tdc.pipeline.context import ContextManager


class UserProvider:
    """用户来源提供者"""

    def __init__(self, config: ExecutionConfig, context_manager: ContextManager):
        self.config = config
        self.context_manager = context_manager
        self._users: List[str] = []

    def initialize(self) -> None:
        """初始化用户列表"""
        if self.config.user_source == "faker":
            # faker 模式：延迟生成，无需初始化
            pass
        elif self.config.user_source == "http":
            self._users = self._fetch_users_from_http()
        elif self.config.user_source == "list":
            self._users = self.config.user_list or []
        else:
            raise UserSourceError(f"Unknown user_source: {self.config.user_source}")

    def get_user(self, iteration: int) -> str:
        """获取第 iteration 个用户"""
        if self.config.user_source == "faker":
            # 每次渲染 template 生成新用户
            return self.context_manager.render_template(
                self.config.user_template or "{{ faker.username }}"
            )
        elif self.config.user_source in ("http", "list"):
            # 从预获取的列表中按序取用
            if not self._users:
                raise UserSourceError(f"No users available from source: {self.config.user_source}")
            # 循环使用：iteration % len(users)
            return self._users[iteration % len(self._users)]
        else:
            raise UserSourceError(f"Unknown user_source: {self.config.user_source}")

    def _fetch_users_from_http(self) -> List[str]:
        """从 HTTP 接口获取用户列表"""
        http_config = self.config.user_http
        if not http_config:
            raise UserSourceError("user_http config required for http source")

        try:
            # 发送 HTTP 请求
            response = httpx.request(
                method=http_config.method,
                url=http_config.url,
                headers=http_config.headers,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            raise UserSourceError(f"Failed to fetch users from HTTP: {e}")
        except Exception as e:
            raise UserSourceError(f"Failed to parse users response: {e}")

        # JSONPath 提取用户列表
        users = self._extract_by_path(data, http_config.user_path)
        if not isinstance(users, list):
            raise UserSourceError(f"Expected list from user_path, got {type(users)}")

        # 如果配置了 user_field，从对象中提取字段
        if http_config.user_field:
            try:
                users = [
                    self._extract_by_path(user, http_config.user_field)
                    for user in users
                ]
            except (KeyError, TypeError) as e:
                raise UserSourceError(f"Failed to extract user_field: {e}")

        return users

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
