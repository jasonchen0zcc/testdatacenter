import pytest
from unittest.mock import AsyncMock, Mock, patch

from tdc.config.models import DBAssertionConfig, DBAssertionMode
from tdc.core.db_assertions import DBAssertionValidator
from tdc.pipeline.context import ContextManager
from tdc.core.models import Context


class TestDBAssertionValidator:
    """DBAssertionValidator 单元测试"""

    @pytest.mark.asyncio
    async def test_validate_expected_rows_success(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT * FROM orders",
            expected_rows=1,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )

        assert result.success is True

    @pytest.mark.asyncio
    async def test_validate_expected_rows_failure(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT * FROM orders",
            expected_rows=2,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "expected 2 rows but got 1" in result.message

    @pytest.mark.asyncio
    async def test_validate_expected_value_success(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT status FROM orders",
            query_path="status",
            expected_value="PENDING",
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"status": "PENDING"}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is True

    @pytest.mark.asyncio
    async def test_validate_table_mode_success(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.TABLE,
            table="orders",
            where='order_no = "123"',
            expected_rows=1,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is True
        calls = mock_conn.execute.call_args_list
        assert "SELECT * FROM orders WHERE" in str(calls[1][0][0])

    @pytest.mark.asyncio
    async def test_validate_engine_not_found(self):
        config = DBAssertionConfig(
            instance="missing_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT 1",
            expected_rows=1,
        )
        pool_manager = Mock()
        pool_manager.get_engine.side_effect = KeyError("missing_db")

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "missing_db" in result.message

    @pytest.mark.asyncio
    async def test_validate_delay_ms(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT 1",
            expected_rows=1,
            delay_ms=100,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        with patch("tdc.core.db_assertions.asyncio.sleep") as mock_sleep:
            result = await DBAssertionValidator.validate(
                config, pool_manager, manager, None, "order_db"
            )
            mock_sleep.assert_called_once_with(0.1)
            assert result.success is True

    @pytest.mark.asyncio
    async def test_validate_retry_success_on_second_attempt(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT 1",
            expected_rows=1,
            retry={"max_attempts": 2, "delay": 0, "backoff": "fixed"},
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(side_effect=[Exception("conn failed"), mock_conn])

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is True
        assert engine.connect.call_count == 2

    @pytest.mark.asyncio
    async def test_validate_query_path_not_found(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT 1",
            query_path="missing",
            expected_value="x",
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "query_path 'missing' not found" in result.message

    @pytest.mark.asyncio
    async def test_validate_timeout_failure(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT SLEEP(10)",
            expected_rows=1,
            timeout=1,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        import asyncio

        _execute_calls = [0]

        def _execute_side_effect(*args, **kwargs):
            _execute_calls[0] += 1
            if _execute_calls[0] % 2 == 0:
                raise asyncio.TimeoutError
            return Mock()

        mock_conn.execute = AsyncMock(side_effect=_execute_side_effect)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "timed out" in result.message.lower()

    @pytest.mark.asyncio
    async def test_validate_invalid_database_identifier(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT 1",
            expected_rows=1,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order-db"  # 含连字符，非法
        )
        assert result.success is False
        assert "Invalid database identifier" in result.message

    @pytest.mark.asyncio
    async def test_validate_invalid_table_identifier(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.TABLE,
            table="orders; DROP",
            where="1=1",
            expected_rows=1,
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "Invalid table identifier" in result.message

    @pytest.mark.asyncio
    async def test_validate_retry_exhausted(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT 1",
            expected_rows=1,
            retry={"max_attempts": 2, "delay": 0, "backoff": "fixed"},
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine
        engine.connect = Mock(side_effect=Exception("always fails"))

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "always fails" in result.message
        assert engine.connect.call_count == 3  # 1 次初始 + 2 次重试

    @pytest.mark.asyncio
    async def test_validate_expected_value_mismatch(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT status FROM orders",
            query_path="status",
            expected_value="PAID",
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = [{"status": "PENDING"}]
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "expected value 'PAID' but got 'PENDING'" in result.message

    @pytest.mark.asyncio
    async def test_validate_no_rows_for_value_assertion(self):
        config = DBAssertionConfig(
            instance="test_db",
            mode=DBAssertionMode.SQL,
            sql="SELECT status FROM orders WHERE 1=0",
            query_path="status",
            expected_value="PENDING",
        )
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.mappings.return_value.all.return_value = []
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        ctx = Context("task_1")
        manager = ContextManager(ctx)

        result = await DBAssertionValidator.validate(
            config, pool_manager, manager, None, "order_db"
        )
        assert result.success is False
        assert "no rows returned for value assertion" in result.message

    def test_compute_retry_delay_exponential(self):
        from tdc.core.db_assertions import _compute_retry_delay
        from tdc.config.models import RetryConfig

        retry = RetryConfig(max_attempts=3, delay=2, backoff="exponential")
        assert _compute_retry_delay(retry, 1) == 2
        assert _compute_retry_delay(retry, 2) == 4
        assert _compute_retry_delay(retry, 3) == 8
