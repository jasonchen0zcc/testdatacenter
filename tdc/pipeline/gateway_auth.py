from typing import Dict, Optional

import httpx
from jsonpath_ng import parse

from tdc.config.models import GatewayConfig
from tdc.config.template_loader import TemplateLoader
from tdc.core.exceptions import GatewayAuthError, TokenExtractionError
from tdc.core.models import ExecutionContext
from tdc.pipeline.context import ContextManager


class GatewayAuth:
    """网关认证管理器"""

    def __init__(
        self,
        config: GatewayConfig,
        task_id: str,
        template_loader: TemplateLoader,
        context_manager: ContextManager
    ):
        self.config = config
        self.task_id = task_id
        self.template_loader = template_loader
        self.context_manager = context_manager
        self.token: Optional[str] = None

    async def authenticate(self, execution: ExecutionContext) -> str:
        """
        执行认证，获取 Token

        Args:
            execution: 当前执行上下文（含 user）

        Returns:
            获取到的 token 字符串
        """
        # 1. 加载并渲染 auth 模板（可访问 execution.user）
        body = self._render_auth_body(execution)

        # 2. 发送认证请求
        response_data = await self._send_auth_request(body)

        # 3. JSONPath 提取 token
        self.token = self._extract_token(response_data)

        return self.token

    def apply_to_request(self, headers: Dict[str, str]) -> Dict[str, str]:
        """将 token 注入请求 headers"""
        if self.token:
            headers[self.config.header_name] = f"{self.config.header_prefix}{self.token}"
        return headers

    def _render_auth_body(self, execution: ExecutionContext) -> str:
        """渲染认证请求体"""
        template = self.template_loader.load_body_template(
            self.config.body_template,
            self.task_id
        )
        # ContextManager 渲染时提供 execution 变量
        return self.context_manager.render_template_with_execution(template, execution)

    async def _send_auth_request(self, body: str) -> dict:
        """发送认证请求，返回响应 JSON"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=self.config.method,
                    url=self.config.auth_url,
                    headers=self.config.headers,
                    content=body,
                    timeout=30
                )
                response.raise_for_status()
                return response.json()
        except httpx.HTTPStatusError as e:
            raise GatewayAuthError(
                f"Gateway authentication failed: {e.response.status_code}",
                status_code=e.response.status_code
            )
        except httpx.RequestError as e:
            raise GatewayAuthError(f"Gateway authentication request failed: {e}")
        except Exception as e:
            raise GatewayAuthError(f"Unexpected error during gateway auth: {e}")

    def _extract_token(self, response: dict) -> str:
        """使用 JSONPath 从响应中提取 token"""
        try:
            jsonpath_expr = parse(self.config.token_path)
            matches = jsonpath_expr.find(response)
            if matches:
                return str(matches[0].value)
            raise TokenExtractionError(
                f"Token not found at path '{self.config.token_path}' in response"
            )
        except TokenExtractionError:
            raise
        except Exception as e:
            raise TokenExtractionError(f"Failed to extract token: {e}")
