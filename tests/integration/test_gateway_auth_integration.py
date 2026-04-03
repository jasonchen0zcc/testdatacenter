import pytest
from unittest.mock import AsyncMock, Mock, patch

from tdc.config.models import (
    ExecutionConfig,
    GatewayConfig,
    HTTPConfig,
    PipelineStepConfig,
    TaskConfig,
    TargetDBConfig,
    UserHttpConfig,
)
from tdc.config.template_loader import TemplateLoader
from tdc.core.constants import TaskType
from tdc.core.models import Context
from tdc.pipeline.engine import PipelineEngine


class TestGatewayAuthIntegration:
    """网关认证集成测试"""

    @pytest.mark.asyncio
    async def test_full_flow_with_gateway_and_execution(self, tmp_path):
        """测试完整流程：网关认证 + 批量执行"""
        # 创建模板目录和文件
        templates_dir = tmp_path / "templates" / "order_flow"
        templates_dir.mkdir(parents=True)

        # 创建认证模板
        auth_template = templates_dir / "auth.json"
        auth_template.write_text('{"username": "{{ execution.user }}"}')

        # 创建业务模板
        business_template = templates_dir / "create_order.json"
        business_template.write_text(
            '{"user": "{{ execution.user }}", "order": "{{ faker.uuid4 }}"}'
        )

        # 创建 TaskConfig
        config = TaskConfig(
            task_id="order_flow",
            task_name="Order Flow",
            task_type=TaskType.HTTP_SOURCE,
            schedule="0 * * * *",
            execution=ExecutionConfig(
                iterations=2,
                user_source="list",
                user_list=["alice", "bob"],
                delay_ms=50,
            ),
            gateway=GatewayConfig(
                auth_url="https://auth.example.com/token",
                body_template="auth.json",
                token_path="access_token",
            ),
            pipeline=[
                PipelineStepConfig(
                    step_id="create_order",
                    http=HTTPConfig(
                        url="https://api.example.com/orders",
                        method="POST",
                        body_template="create_order.json",
                    ),
                )
            ],
            target_db=TargetDBConfig(instance="test", database="test_db"),
        )

        # 创建 TemplateLoader（指向 templates 的父目录，即 tmp_path）
        template_loader = TemplateLoader(str(tmp_path))

        # 创建 Engine
        engine = PipelineEngine(template_loader)

        with patch("tdc.pipeline.gateway_auth.httpx.AsyncClient") as mock_client_class:
            with patch.object(
                engine.http_client, "request", new_callable=AsyncMock
            ) as mock_http_request:
                # 模拟认证响应
                mock_auth_response = Mock()
                mock_auth_response.json.return_value = {"access_token": "token_123"}
                mock_auth_response.raise_for_status = Mock()

                mock_client = Mock()
                mock_client.request = AsyncMock(return_value=mock_auth_response)
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_class.return_value = mock_client

                # 模拟业务响应
                mock_response = Mock()
                mock_response.status_code = 201
                mock_response.json.return_value = {"order_id": "123"}
                mock_http_request.return_value = mock_response

                ctx = Context(config.task_id)
                result = await engine.execute(config, ctx)

                assert result.success is True

                # 验证认证调用
                assert mock_client.request.call_count == 2
                # 验证业务请求调用
                assert mock_http_request.call_count == 2

                # 验证请求头包含 token（HTTPConfig 的 headers 属性）
                for call in mock_http_request.call_args_list:
                    config = call.args[0]  # 第一个参数是 HTTPConfig
                    assert config.headers.get("Authorization") == "Bearer token_123"


class TestHttpUserSourceIntegration:
    """HTTP 用户来源集成测试"""

    @pytest.mark.asyncio
    async def test_http_user_source_flow(self, tmp_path):
        """测试从 HTTP 获取用户列表的完整流程"""
        templates_dir = tmp_path / "templates" / "test_task"
        templates_dir.mkdir(parents=True)

        template = templates_dir / "step.json"
        template.write_text('{"user": "{{ execution.user }}"}')

        config = TaskConfig(
            task_id="test_task",
            task_name="Test",
            task_type=TaskType.HTTP_SOURCE,
            schedule="0 * * * *",
            execution=ExecutionConfig(
                iterations=3,
                user_source="http",
                user_http=UserHttpConfig(
                    url="https://api.example.com/users",
                    user_path="data.users",
                    user_field="username",
                ),
            ),
            pipeline=[
                PipelineStepConfig(
                    step_id="step1",
                    http=HTTPConfig(
                        url="https://api.example.com/action",
                        method="POST",
                        body_template="step.json",
                    ),
                )
            ],
            target_db=TargetDBConfig(instance="test", database="test_db"),
        )

        template_loader = TemplateLoader(str(tmp_path))
        engine = PipelineEngine(template_loader)

        with patch("tdc.pipeline.user_provider.httpx.request") as mock_user_request:
            with patch.object(
                engine.http_client, "request", new_callable=AsyncMock
            ) as mock_http_request:
                # 模拟用户列表响应
                mock_user_response = Mock()
                mock_user_response.json.return_value = {
                    "data": {
                        "users": [
                            {"id": 1, "username": "user_a"},
                            {"id": 2, "username": "user_b"},
                        ]
                    }
                }
                mock_user_response.raise_for_status = Mock()
                mock_user_request.return_value = mock_user_response

                # 模拟业务响应
                mock_response = Mock()
                mock_response.status_code = 200
                mock_response.json.return_value = {}
                mock_http_request.return_value = mock_response

                ctx = Context(config.task_id)
                result = await engine.execute(config, ctx)

                assert result.success is True
                # 用户列表请求 1 次
                assert mock_user_request.call_count == 1
                # 业务请求 3 次（iterations）
                assert mock_http_request.call_count == 3


class TestMultiStepGatewayAuthIntegration:
    """多步网关认证集成测试"""

    @pytest.mark.asyncio
    async def test_two_step_token_exchange_flow(self, tmp_path):
        """测试 once-token → exchange-token 的完整流程"""
        templates_dir = tmp_path / "templates" / "multi_auth_flow"
        templates_dir.mkdir(parents=True)

        (templates_dir / "once_token.json").write_text(
            '{"username": "{{ execution.user }}"}'
        )
        (templates_dir / "exchange_token.json").write_text(
            '{"username": "{{ execution.user }}", "onceToken": "{{ auth_context.onceToken }}"}'
        )
        (templates_dir / "create_order.json").write_text(
            '{"user": "{{ execution.user }}"}'
        )

        config = TaskConfig(
            task_id="multi_auth_flow",
            task_name="Multi Auth Flow",
            task_type=TaskType.HTTP_SOURCE,
            schedule="0 * * * *",
            execution=ExecutionConfig(
                iterations=1, user_source="list", user_list=["alice"], delay_ms=50
            ),
            gateway=GatewayConfig(
                steps=[
                    {
                        "name": "get_once_token",
                        "auth_url": "https://auth.example.com/once-token",
                        "body_template": "once_token.json",
                        "token_path": "data.onceToken",
                        "extract_to": "onceToken",
                    },
                    {
                        "name": "exchange_token",
                        "auth_url": "https://auth.example.com/exchange-token",
                        "body_template": "exchange_token.json",
                        "token_path": "data.token",
                    },
                ],
                header_name="Authorization",
                header_prefix="Bearer ",
            ),
            pipeline=[
                PipelineStepConfig(
                    step_id="create_order",
                    http=HTTPConfig(
                        url="https://api.example.com/orders",
                        method="POST",
                        body_template="create_order.json",
                    ),
                )
            ],
            target_db=TargetDBConfig(instance="test", database="test_db"),
        )

        template_loader = TemplateLoader(str(tmp_path))
        engine = PipelineEngine(template_loader)

        with patch("tdc.pipeline.gateway_auth.httpx.AsyncClient") as mock_client_class:
            with patch.object(
                engine.http_client, "request", new_callable=AsyncMock
            ) as mock_http_request:
                # 模拟 once-token 响应
                once_response = Mock()
                once_response.json.return_value = {"data": {"onceToken": "once_abc"}}
                once_response.raise_for_status = Mock()

                # 模拟 exchange-token 响应
                exchange_response = Mock()
                exchange_response.json.return_value = {"data": {"token": "real_xyz"}}
                exchange_response.raise_for_status = Mock()

                mock_client = Mock()
                mock_client.request = AsyncMock(
                    side_effect=[once_response, exchange_response]
                )
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_client_class.return_value = mock_client

                # 模拟业务响应
                business_response = Mock()
                business_response.status_code = 201
                business_response.json.return_value = {"order_id": "123"}
                mock_http_request.return_value = business_response

                ctx = Context(config.task_id)
                result = await engine.execute(config, ctx)

                assert result.success is True

                # 验证认证链被调用 2 次
                assert mock_client.request.call_count == 2

                # 验证第二步请求体包含 onceToken
                second_call = mock_client.request.call_args_list[1]
                body = second_call.kwargs.get("content")
                assert "once_abc" in body

                # 验证业务请求头包含最终 token
                business_call = mock_http_request.call_args
                http_config = business_call.args[0]
                assert http_config.headers.get("Authorization") == "Bearer real_xyz"
