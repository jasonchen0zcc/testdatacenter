import pytest
from unittest.mock import AsyncMock, Mock, patch

from tdc.config.models import (
    DBAssertionConfig,
    DBAssertionMode,
    ExecutionConfig,
    HTTPConfig,
    PipelineStepConfig,
    TargetDBConfig,
    TaskConfig,
)
from tdc.config.template_loader import TemplateLoader
from tdc.core.assertions import AssertionResult
from tdc.core.constants import TaskType
from tdc.core.models import Context
from tdc.pipeline.engine import PipelineEngine
from tdc.storage.mysql_pool import MySQLPoolManager


class TestDbAssertionIntegration:
    """DB 断言集成测试"""

    @pytest.mark.asyncio
    async def test_full_step_with_db_assertion_success(self, tmp_path):
        templates_dir = tmp_path / "templates" / "db_flow"
        templates_dir.mkdir(parents=True)
        (templates_dir / "step.json").write_text('{"ref": "{{ context.get(\'orderNo\') }}"}')

        config = TaskConfig(
            task_id="db_flow",
            task_name="DB Assertion Flow",
            task_type=TaskType.HTTP_SOURCE,
            schedule="0 * * * *",
            execution=ExecutionConfig(iterations=1, user_source="list", user_list=["alice"]),
            pipeline=[
                PipelineStepConfig(
                    step_id="create_order",
                    http=HTTPConfig(
                        url="https://api.example.com/orders",
                        method="POST",
                        body_template="step.json"
                    ),
                    extract={"orderNo": "data.orderNo"},
                    db_assertions=[
                        DBAssertionConfig(
                            instance="test_db",
                            database="order_db",
                            mode=DBAssertionMode.SQL,
                            sql='SELECT order_no FROM orders WHERE order_no = "{{ context.get(\'orderNo\') }}"',
                            expected_rows=1,
                            delay_ms=50,
                        )
                    ]
                )
            ],
            target_db=TargetDBConfig(instance="test_db", database="order_db"),
        )

        template_loader = TemplateLoader(str(tmp_path))
        pool_manager = MySQLPoolManager()
        pool_manager.register("test_db", "mysql+aiomysql://user:pass@localhost:3306", pool_size=2)

        engine = PipelineEngine(template_loader, pool_manager=pool_manager, default_database="order_db")

        with patch.object(engine.http_client, 'request', new_callable=AsyncMock) as mock_http:
            mock_resp = Mock()
            mock_resp.status_code = 200
            mock_resp.json.return_value = {"data": {"orderNo": "ORD-123"}}
            mock_http.return_value = mock_resp

            with patch("tdc.pipeline.engine.DBAssertionValidator.validate", new_callable=AsyncMock) as mock_db:
                mock_db.return_value = AssertionResult(success=True)

                ctx = Context(config.task_id)
                result = await engine.execute(config, ctx)

                assert result.success is True
                assert mock_db.call_count == 1
                # 验证模板变量被正确渲染
                call_kwargs = mock_db.call_args.kwargs
                assert call_kwargs["default_database"] == "order_db"
                assert call_kwargs["pool_manager"] is pool_manager
                assert call_kwargs["config"].sql == 'SELECT order_no FROM orders WHERE order_no = "{{ context.get(\'orderNo\') }}"'

        await engine.close()
