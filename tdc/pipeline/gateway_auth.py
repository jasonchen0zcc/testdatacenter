from typing import Dict, Optional

import httpx
from jsonpath_ng import parse

from tdc.config.models import GatewayConfig, GatewayStepConfig
from tdc.config.template_loader import TemplateLoader
from tdc.core.exceptions import GatewayAuthError, TokenExtractionError
from tdc.core.models import ExecutionContext
from tdc.pipeline.context import ContextManager


class GatewayAuth:
    """网关认证管理器，支持单步或多步认证链"""

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
        执行认证链，获取最终 Token
        """
        auth_context: Dict[str, str] = {}

        if not self.config.steps:
            raise GatewayAuthError("Gateway authentication steps are not configured")

        for step in self.config.steps:
            # 1. 加载并渲染模板
            body = self._render_auth_body(step, execution, auth_context)

            # 2. 发送认证请求
            response_data = await self._send_auth_request(step, body)

            # 3. 提取值
            value = self._extract_token(step, response_data)

            if step.extract_to:
                auth_context[step.extract_to] = value
            else:
                self.token = value

        if self.token is None:
            raise GatewayAuthError(
                "No final token produced: ensure one step has no extract_to"
            )

        return self.token

    def apply_to_request(self, headers: Dict[str, str]) -> Dict[str, str]:
        """将 token 注入请求 headers"""
        if self.token:
            headers[self.config.header_name] = f"{self.config.header_prefix}{self.token}"
        return headers

    def _render_auth_body(
        self,
        step: "GatewayStepConfig",
        execution: ExecutionContext,
        auth_context: Dict[str, str]
    ) -> str:
        """渲染认证请求体"""
        template = self.template_loader.load_body_template(
            step.body_template,
            self.task_id
        )
        return self.context_manager.render_template_with_execution_and_context(
            template, execution, auth_context
        )

    async def _send_auth_request(self, step: "GatewayStepConfig", body: str) -> dict:
        """发送认证请求，返回响应 JSON"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.request(
                    method=step.method,
                    url=step.auth_url,
                    headers=step.headers,
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
            raise GatewayAuthError(f"Unexpected error during gateway auth: {e}") from e

    def _extract_token(self, step: "GatewayStepConfig", response: dict) -> str:
        """使用 JSONPath 从响应中提取值"""
        try:
            jsonpath_expr = parse(step.token_path)
            matches = jsonpath_expr.find(response)
            if matches:
                return str(matches[0].value)
            raise TokenExtractionError(
                f"Token not found at path '{step.token_path}' in response"
            )
        except TokenExtractionError:
            raise
        except Exception as e:
            raise TokenExtractionError(f"Failed to extract token: {e}") from e
