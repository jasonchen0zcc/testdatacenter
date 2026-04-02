import pytest
from unittest.mock import AsyncMock, Mock, patch

from tdc.config.models import (
    ExecutionConfig,
    GatewayConfig,
    HTTPConfig,
    PipelineStepConfig,
    TaskConfig,
    TargetDBConfig
)
from tdc.config.template_loader import TemplateLoader
from tdc.core.constants import TaskType
from tdc.core.models import Context
from tdc.pipeline.engine import PipelineEngine


class TestPipelineEngineIterations:
    """测试 PipelineEngine 迭代执行"""

    @pytest.fixture
    def base_task_config(self):
        """基础 TaskConfig"""
        return TaskConfig(
            task_id="test_task",
            task_name="Test Task",
            task_type=TaskType.HTTP_SOURCE,
            schedule="0 * * * *",
            target_db=TargetDBConfig(instance="test", database="test_db"),
            pipeline=[
                PipelineStepConfig(
                    step_id="step1",
                    http=HTTPConfig(url="https://api.example.com/test", method="GET")
                )
            ]
        )

    @pytest.fixture
    def template_loader(self):
        """模拟 TemplateLoader"""
        loader = Mock(spec=TemplateLoader)
        loader.load_body_template = Mock(return_value="{}")
        return loader

    @pytest.mark.asyncio
    async def test_single_iteration_default(self, base_task_config, template_loader):
        """测试默认单迭代执行"""
        engine = PipelineEngine(template_loader)

        with patch.object(engine.http_client, 'request', new_callable=AsyncMock) as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {}
            mock_request.return_value = mock_response

            ctx = Context(base_task_config.task_id)
            result = await engine.execute(base_task_config, ctx)

            assert result.success is True
            assert mock_request.call_count == 1

    @pytest.mark.asyncio
    async def test_multiple_iterations(self, base_task_config, template_loader):
        """测试多迭代执行"""
        base_task_config.execution = ExecutionConfig(
            iterations=3,
            user_source="list",
            user_list=["user1", "user2", "user3"]
        )

        engine = PipelineEngine(template_loader)

        with patch.object(engine.http_client, 'request', new_callable=AsyncMock) as mock_request:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {}
            mock_request.return_value = mock_response

            ctx = Context(base_task_config.task_id)
            result = await engine.execute(base_task_config, ctx)

            assert result.success is True
            assert mock_request.call_count == 3  # 3 次迭代

    @pytest.mark.asyncio
    async def test_iterations_with_delay(self, base_task_config, template_loader):
        """测试迭代延迟"""
        base_task_config.execution = ExecutionConfig(
            iterations=2,
            user_source="list",
            user_list=["user1", "user2"],
            delay_ms=100  # 100ms 延迟
        )

        engine = PipelineEngine(template_loader)

        with patch.object(engine.http_client, 'request', new_callable=AsyncMock) as mock_request:
            with patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {}
                mock_request.return_value = mock_response

                ctx = Context(base_task_config.task_id)
                await engine.execute(base_task_config, ctx)

                # 验证 sleep 被调用 1 次（最后一次不 sleep）
                assert mock_sleep.call_count == 1
                mock_sleep.assert_called_with(0.1)  # 100ms = 0.1s

    @pytest.mark.asyncio
    async def test_iteration_with_gateway_auth(self, base_task_config, template_loader):
        """测试带网关认证的迭代"""
        base_task_config.execution = ExecutionConfig(
            iterations=2,
            user_source="list",
            user_list=["alice", "bob"]
        )
        base_task_config.gateway = GatewayConfig(
            auth_url="https://auth.example.com/token",
            body_template="auth.json",
            token_path="token"
        )

        engine = PipelineEngine(template_loader)

        with patch("tdc.pipeline.gateway_auth.httpx.AsyncClient") as mock_client_class:
            with patch.object(engine.http_client, 'request', new_callable=AsyncMock) as mock_http_request:
                # 模拟认证响应
                mock_auth_response = Mock()
                mock_auth_response.json.return_value = {"token": "test_token"}
                mock_auth_response.raise_for_status = Mock()

                mock_client = Mock()
                mock_client.request = AsyncMock(return_value=mock_auth_response)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_class.return_value = mock_client

                # 模拟 HTTP 响应
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {}
                mock_http_request.return_value = mock_response

                ctx = Context(base_task_config.task_id)
                result = await engine.execute(base_task_config, ctx)

                assert result.success is True
                # 认证 2 次（每轮迭代）
                assert mock_client.request.call_count == 2
                # HTTP 请求 2 次
                assert mock_http_request.call_count == 2
