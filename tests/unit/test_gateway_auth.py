import pytest
from unittest.mock import AsyncMock, Mock, patch

from tdc.config.models import GatewayConfig, GatewayStepConfig
from tdc.config.template_loader import TemplateLoader
from tdc.core.exceptions import GatewayAuthError, TokenExtractionError
from tdc.core.models import ExecutionContext
from tdc.pipeline.context import ContextManager
from tdc.pipeline.gateway_auth import GatewayAuth


class TestGatewayAuth:
    """测试 GatewayAuth"""

    def create_gateway_auth(self, token_path="data.access_token", steps=None):
        """辅助方法创建 GatewayAuth 实例"""
        if steps is None:
            config = GatewayConfig(
                auth_url="https://gateway.example.com/oauth/token",
                method="POST",
                body_template="auth.json",
                token_path=token_path,
                header_name="Authorization",
                header_prefix="Bearer "
            )
        else:
            config = GatewayConfig(
                steps=steps,
                header_name="Authorization",
                header_prefix="Bearer "
            )
        template_loader = Mock(spec=TemplateLoader)
        context = Mock()
        context_manager = ContextManager(context)

        auth = GatewayAuth(config, "test_task", template_loader, context_manager)
        return auth, template_loader, context_manager

    @pytest.mark.asyncio
    @patch("tdc.pipeline.gateway_auth.httpx.AsyncClient")
    async def test_authenticate_success(self, mock_client_class):
        """测试认证成功获取 token"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {"access_token": "test_token_123"}
        }
        mock_response.raise_for_status = Mock()

        mock_client = Mock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        auth, template_loader, _ = self.create_gateway_auth()
        template_loader.load_body_template.return_value = '{"username": "{{ execution.user }}"}'

        execution = ExecutionContext(iteration=0, user="testuser", total=1)
        token = await auth.authenticate(execution)

        assert token == "test_token_123"
        assert auth.token == "test_token_123"

    @pytest.mark.asyncio
    @patch("tdc.pipeline.gateway_auth.httpx.AsyncClient")
    async def test_authenticate_custom_token_path(self, mock_client_class):
        """测试自定义 token 路径"""
        mock_response = Mock()
        mock_response.json.return_value = {
            "result": {"token": "custom_token"}
        }
        mock_response.raise_for_status = Mock()

        mock_client = Mock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        auth, template_loader, _ = self.create_gateway_auth(token_path="result.token")
        template_loader.load_body_template.return_value = '{}'

        execution = ExecutionContext(iteration=0, user="testuser", total=1)
        token = await auth.authenticate(execution)

        assert token == "custom_token"

    @pytest.mark.asyncio
    @patch("tdc.pipeline.gateway_auth.httpx.AsyncClient")
    async def test_authenticate_http_error(self, mock_client_class):
        """测试 HTTP 错误抛出 GatewayAuthError"""
        import httpx

        mock_response = Mock()
        mock_response.status_code = 401

        mock_client = Mock()
        mock_client.request = AsyncMock(side_effect=httpx.HTTPStatusError(
            "Unauthorized",
            request=Mock(),
            response=mock_response
        ))
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        auth, template_loader, _ = self.create_gateway_auth()
        template_loader.load_body_template.return_value = '{}'

        execution = ExecutionContext(iteration=0, user="testuser", total=1)

        with pytest.raises(GatewayAuthError, match="Gateway authentication failed: 401"):
            await auth.authenticate(execution)

    @pytest.mark.asyncio
    @patch("tdc.pipeline.gateway_auth.httpx.AsyncClient")
    async def test_token_extraction_failure(self, mock_client_class):
        """测试 token 提取失败抛出 TokenExtractionError"""
        mock_response = Mock()
        mock_response.json.return_value = {"data": {}}  # 缺少 access_token
        mock_response.raise_for_status = Mock()

        mock_client = Mock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        auth, template_loader, _ = self.create_gateway_auth()
        template_loader.load_body_template.return_value = '{}'

        execution = ExecutionContext(iteration=0, user="testuser", total=1)

        with pytest.raises(TokenExtractionError, match="Token not found"):
            await auth.authenticate(execution)

    def test_apply_to_request(self):
        """测试 token 注入 headers"""
        auth, _, _ = self.create_gateway_auth()
        auth.token = "my_token"

        headers = auth.apply_to_request({"Content-Type": "application/json"})

        assert headers["Authorization"] == "Bearer my_token"
        assert headers["Content-Type"] == "application/json"

    def test_apply_to_request_no_token(self):
        """测试无 token 时不修改 headers"""
        auth, _, _ = self.create_gateway_auth()
        auth.token = None

        headers = auth.apply_to_request({"Content-Type": "application/json"})

        assert "Authorization" not in headers
        assert headers["Content-Type"] == "application/json"

    @pytest.mark.asyncio
    @patch("tdc.pipeline.gateway_auth.httpx.AsyncClient")
    async def test_two_step_authenticate_success(self, mock_client_class):
        """测试两步认证成功获取最终 token"""
        # 第一步返回 onceToken，第二步返回 realToken
        responses = [
            {"data": {"onceToken": "once_123"}},
            {"data": {"token": "real_token_456"}},
        ]

        mock_client = Mock()
        mock_client.request = AsyncMock(
            side_effect=[
                Mock(json=lambda r=r: r, raise_for_status=Mock()) for r in responses
            ]
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        steps = [
            GatewayStepConfig(
                auth_url="https://gateway.example.com/once-token",
                body_template="once.json",
                token_path="data.onceToken",
                extract_to="onceToken"
            ),
            GatewayStepConfig(
                auth_url="https://gateway.example.com/exchange-token",
                body_template="exchange.json",
                token_path="data.token"
            ),
        ]
        auth, template_loader, _ = self.create_gateway_auth(steps=steps)
        template_loader.load_body_template.side_effect = lambda t, task_id: {
            "once.json": '{"user":"{{ execution.user }}"}',
            "exchange.json": '{"user":"{{ execution.user }}","onceToken":"{{ auth_context.onceToken }}"}'
        }.get(t, t)

        execution = ExecutionContext(iteration=0, user="testuser", total=1)
        token = await auth.authenticate(execution)

        assert token == "real_token_456"
        assert auth.token == "real_token_456"

        # 验证第二步模板中使用了 onceToken
        second_call = mock_client.request.call_args_list[1]
        body = second_call.kwargs.get("content") or second_call.args[0]
        assert "once_123" in body

    @pytest.mark.asyncio
    @patch("tdc.pipeline.gateway_auth.httpx.AsyncClient")
    async def test_two_step_all_extract_to_raises_error(self, mock_client_class):
        """测试所有步骤都有 extract_to 时抛出错误"""
        mock_client = Mock()
        mock_client.request = AsyncMock(
            return_value=Mock(json=lambda: {"data": {"token": "t"}}, raise_for_status=Mock())
        )
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client_class.return_value = mock_client

        steps = [
            GatewayStepConfig(
                auth_url="https://gateway.example.com/step1",
                body_template="s1.json",
                token_path="data.token",
                extract_to="temp"
            ),
        ]
        auth, template_loader, _ = self.create_gateway_auth(steps=steps)
        template_loader.load_body_template.return_value = "{}"

        execution = ExecutionContext(iteration=0, user="testuser", total=1)
        with pytest.raises(GatewayAuthError, match="No final token produced"):
            await auth.authenticate(execution)
