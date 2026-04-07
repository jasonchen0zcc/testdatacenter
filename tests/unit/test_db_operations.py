import pytest
from unittest.mock import AsyncMock, Mock

from tdc.config.models import (
    DBOperationMode,
    DBOperationType,
    SingleDBOperationConfig,
    TransactionDBOperationConfig,
)
from tdc.storage.db_operations import DBOperationExecutor, DBOperationResult
from tdc.pipeline.context import ContextManager
from tdc.core.models import Context, ExecutionContext


class TestDBOperationResult:
    """DBOperationResult 单元测试"""

    def test_result_creation(self):
        result = DBOperationResult(success=True, rowcount=5, message="OK")
        assert result.success is True
        assert result.rowcount == 5
        assert result.message == "OK"
        assert result.results == []

    def test_result_with_nested_results(self):
        nested = DBOperationResult(success=True, rowcount=3)
        result = DBOperationResult(
            success=True,
            results=[nested, DBOperationResult(success=False, message="error")]
        )
        assert len(result.results) == 2
        assert result.results[0].rowcount == 3


class TestDBOperationExecutorBuildSQL:
    """DBOperationExecutor._build_sql 单元测试"""

    @pytest.fixture
    def executor(self):
        pool_manager = Mock()
        return DBOperationExecutor(pool_manager)

    def test_build_sql_update_table_mode(self, executor):
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            table="orders",
            set={"status": "PAID", "updated_at": "2024-01-01"},
            where="id = 1",
        )
        sql = executor._build_sql(config)
        assert sql == "UPDATE `orders` SET `status` = :status, `updated_at` = :updated_at WHERE id = 1"

    def test_build_sql_delete_table_mode(self, executor):
        config = SingleDBOperationConfig(
            type=DBOperationType.DELETE,
            instance="test_db",
            table="orders",
            where="id = 1",
        )
        sql = executor._build_sql(config)
        assert sql == "DELETE FROM `orders` WHERE id = 1"

    def test_build_sql_sql_mode(self, executor):
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            mode=DBOperationMode.SQL,
            sql="UPDATE orders SET status = 'PAID' WHERE id = 1",
        )
        sql = executor._build_sql(config)
        assert sql == "UPDATE orders SET status = 'PAID' WHERE id = 1"

    def test_build_sql_empty_sql_mode(self, executor):
        # When mode=SQL, the sql field is required by model validation
        # Test with empty string instead
        config = SingleDBOperationConfig(
            type=DBOperationType.DELETE,
            instance="test_db",
            mode=DBOperationMode.SQL,
            sql="",
        )
        sql = executor._build_sql(config)
        assert sql == ""


class TestDBOperationExecutorRenderSQL:
    """DBOperationExecutor._render_sql 单元测试"""

    @pytest.fixture
    def executor(self):
        pool_manager = Mock()
        return DBOperationExecutor(pool_manager)

    @pytest.fixture
    def context_manager(self):
        ctx = Context("task_1")
        ctx.set("order_no", "ORD123")
        return ContextManager(ctx)

    def test_render_sql_without_execution(self, executor, context_manager):
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            mode=DBOperationMode.SQL,
            sql='UPDATE orders SET status = "{{ context.get(\"order_no\") }}"',
        )
        sql = executor._render_sql(config, context_manager, None)
        assert sql == 'UPDATE orders SET status = "ORD123"'

    def test_render_sql_with_execution(self, executor, context_manager):
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            mode=DBOperationMode.SQL,
            sql='UPDATE orders SET user = "{{ execution.user }}"',
        )
        execution = ExecutionContext(iteration=0, user="testuser", total=10)
        sql = executor._render_sql(config, context_manager, execution)
        assert sql == 'UPDATE orders SET user = "testuser"'

    def test_render_sql_table_mode(self, executor, context_manager):
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            table="orders",
            set={"status": "{{ context.get('order_no') }}"},
            where='id = 1',
        )
        sql = executor._render_sql(config, context_manager, None)
        assert sql == "UPDATE `orders` SET `status` = :status WHERE id = 1"
        # Note: The template in set values is not rendered in _build_sql,
        # it would be rendered by the database driver with params


class TestDBOperationExecutorExpandBatchParams:
    """DBOperationExecutor._expand_batch_params 单元测试"""

    @pytest.fixture
    def executor(self):
        pool_manager = Mock()
        return DBOperationExecutor(pool_manager)

    @pytest.fixture
    def context(self):
        ctx = Context("task_1")
        ctx.set("order_ids", [1, 2, 3])
        return ctx

    def test_expand_batch_params_with_list(self, executor, context):
        sql = "UPDATE orders SET status = 'PAID' WHERE id IN (:order_ids)"
        params = {}
        batch_params = {"order_ids": "order_ids"}

        new_sql, new_params = executor._expand_batch_params(sql, params, batch_params, context)

        assert new_sql == "UPDATE orders SET status = 'PAID' WHERE id IN (:order_ids_0, :order_ids_1, :order_ids_2)"
        assert new_params == {"order_ids_0": 1, "order_ids_1": 2, "order_ids_2": 3}

    def test_expand_batch_params_with_single_value(self, executor, context):
        context.set("single_id", 42)
        sql = "UPDATE orders SET status = 'PAID' WHERE id IN (:order_ids)"
        params = {}
        batch_params = {"order_ids": "single_id"}

        new_sql, new_params = executor._expand_batch_params(sql, params, batch_params, context)

        assert new_sql == "UPDATE orders SET status = 'PAID' WHERE id IN (:order_ids_0)"
        assert new_params == {"order_ids_0": 42}

    def test_expand_batch_params_empty_list(self, executor, context):
        context.set("empty_ids", [])
        sql = "UPDATE orders SET status = 'PAID' WHERE id IN (:order_ids)"
        params = {}
        batch_params = {"order_ids": "empty_ids"}

        new_sql, new_params = executor._expand_batch_params(sql, params, batch_params, context)

        assert new_sql == "UPDATE orders SET status = 'PAID' WHERE id IN ()"
        assert new_params == {}

    def test_expand_batch_params_multiple_params(self, executor, context):
        context.set("ids_a", [1, 2])
        context.set("ids_b", [10, 20, 30])
        sql = "UPDATE orders SET status = 'PAID' WHERE id IN (:ids_a) OR parent_id IN (:ids_b)"
        params = {}
        batch_params = {"ids_a": "ids_a", "ids_b": "ids_b"}

        new_sql, new_params = executor._expand_batch_params(sql, params, batch_params, context)

        assert ":ids_a_0, :ids_a_1" in new_sql
        assert ":ids_b_0, :ids_b_1, :ids_b_2" in new_sql
        assert new_params["ids_a_0"] == 1
        assert new_params["ids_a_1"] == 2
        assert new_params["ids_b_0"] == 10
        assert new_params["ids_b_1"] == 20
        assert new_params["ids_b_2"] == 30


class TestDBOperationExecutorExecuteSingle:
    """DBOperationExecutor._execute_single 单元测试"""

    @pytest.fixture
    def context_manager(self):
        ctx = Context("task_1")
        return ContextManager(ctx)

    @pytest.mark.asyncio
    async def test_execute_single_success(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 5
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            database="orders_db",
            table="orders",
            set={"status": "PAID"},
            where="id = 1",
        )

        result = await executor._execute_single(
            config, context_manager, None, "default_db"
        )

        assert result.success is True
        assert result.rowcount == 5
        pool_manager.get_engine.assert_called_once_with("test_db")

    @pytest.mark.asyncio
    async def test_execute_single_with_default_database(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.DELETE,
            instance="test_db",
            # No database specified, should use default_database
            table="orders",
            where="id = 1",
        )

        result = await executor._execute_single(
            config, context_manager, None, "default_db"
        )

        assert result.success is True
        # Verify USE statement was executed with default database
        calls = mock_conn.execute.call_args_list
        assert any("USE" in str(call[0][0]) and "default_db" in str(call[0][0]) for call in calls)

    @pytest.mark.asyncio
    async def test_execute_single_with_extract(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 10
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            table="orders",
            set={"status": "ARCHIVED"},
            where="created_at < '2024-01-01'",
            extract={"archived_count": "rowcount"},
        )

        result = await executor._execute_single(
            config, context_manager, None, None
        )

        assert result.success is True
        assert result.rowcount == 10
        assert context_manager.context.get("archived_count") == 10

    @pytest.mark.asyncio
    async def test_execute_single_with_params(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            mode=DBOperationMode.SQL,
            sql="UPDATE orders SET status = :status WHERE id = :id",
            params={"status": "PAID", "id": 123},
        )

        result = await executor._execute_single(
            config, context_manager, None, None
        )

        assert result.success is True
        # Verify execute was called with params
        calls = mock_conn.execute.call_args_list
        # Last call should be the actual SQL execution
        last_call = calls[-1]
        # Check the call arguments - params passed as second positional arg or kwargs
        call_args = last_call[0]
        call_kwargs = last_call[1]
        if "parameters" in call_kwargs:
            assert call_kwargs["parameters"] == {"status": "PAID", "id": 123}
        else:
            # Parameters passed as second positional argument
            assert len(call_args) >= 2
            assert call_args[1] == {"status": "PAID", "id": 123}

    @pytest.mark.asyncio
    async def test_execute_single_with_batch_params(self, context_manager):
        context_manager.context.set("order_ids", [1, 2, 3])

        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            mode=DBOperationMode.SQL,
            sql="UPDATE orders SET status = 'PAID' WHERE id IN (:order_ids)",
            batch_params={"order_ids": "order_ids"},
        )

        result = await executor._execute_single(
            config, context_manager, None, None
        )

        assert result.success is True
        # Verify execute was called with expanded params
        calls = mock_conn.execute.call_args_list
        last_call = calls[-1]
        # Check the call arguments - params passed as second positional arg or kwargs
        call_args = last_call[0]
        call_kwargs = last_call[1]
        if "parameters" in call_kwargs:
            params = call_kwargs["parameters"]
        else:
            params = call_args[1] if len(call_args) >= 2 else {}
        assert "order_ids_0" in params
        assert "order_ids_1" in params
        assert "order_ids_2" in params

    @pytest.mark.asyncio
    async def test_execute_single_engine_not_found(self, context_manager):
        pool_manager = Mock()
        pool_manager.get_engine.side_effect = KeyError("unknown_db")

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="unknown_db",
            table="orders",
            set={"status": "PAID"},
            where="id = 1",
        )

        result = await executor._execute_single(
            config, context_manager, None, None
        )

        assert result.success is False
        assert "KeyError" in result.message
        assert "unknown_db" in result.message

    @pytest.mark.asyncio
    async def test_execute_single_sql_error(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_conn.execute = AsyncMock(side_effect=Exception("SQL syntax error"))
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = SingleDBOperationConfig(
            type=DBOperationType.UPDATE,
            instance="test_db",
            table="orders",
            set={"status": "PAID"},
            where="id = 1",
        )

        result = await executor._execute_single(
            config, context_manager, None, None
        )

        assert result.success is False
        assert "SQL syntax error" in result.message


class TestDBOperationExecutorExecuteTransaction:
    """DBOperationExecutor._execute_transaction 单元测试"""

    @pytest.fixture
    def context_manager(self):
        ctx = Context("task_1")
        return ContextManager(ctx)

    @pytest.mark.asyncio
    async def test_execute_transaction_success(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.begin = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = TransactionDBOperationConfig(
            transaction=True,
            operations=[
                SingleDBOperationConfig(
                    type=DBOperationType.UPDATE,
                    instance="test_db",
                    database="orders_db",
                    table="accounts",
                    set={"balance": "balance - 100"},
                    where="id = 1",
                ),
                SingleDBOperationConfig(
                    type=DBOperationType.UPDATE,
                    instance="test_db",
                    database="orders_db",
                    table="accounts",
                    set={"balance": "balance + 100"},
                    where="id = 2",
                ),
            ],
        )

        result = await executor._execute_transaction(
            config, context_manager, None, "default_db"
        )

        assert result.success is True
        assert result.rowcount == 2  # Sum of both operations

    @pytest.mark.asyncio
    async def test_execute_transaction_empty_operations(self, context_manager):
        pool_manager = Mock()
        executor = DBOperationExecutor(pool_manager)
        config = TransactionDBOperationConfig(
            transaction=True,
            operations=[],
        )

        result = await executor._execute_transaction(
            config, context_manager, None, None
        )

        assert result.success is True
        assert result.rowcount == 0

    @pytest.mark.asyncio
    async def test_execute_transaction_rollback_on_error(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        # First call succeeds, second fails
        mock_conn.execute = AsyncMock(side_effect=[mock_result, Exception("Deadlock")])
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.begin = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = TransactionDBOperationConfig(
            transaction=True,
            operations=[
                SingleDBOperationConfig(
                    type=DBOperationType.UPDATE,
                    instance="test_db",
                    table="accounts",
                    set={"balance": "balance - 100"},
                    where="id = 1",
                ),
                SingleDBOperationConfig(
                    type=DBOperationType.UPDATE,
                    instance="test_db",
                    table="accounts",
                    set={"balance": "balance + 100"},
                    where="id = 2",
                ),
            ],
        )

        result = await executor._execute_transaction(
            config, context_manager, None, None
        )

        assert result.success is False
        assert "Transaction failed" in result.message
        assert "Deadlock" in result.message

    @pytest.mark.asyncio
    async def test_execute_transaction_with_batch_params(self, context_manager):
        context_manager.context.set("ids", [1, 2, 3])

        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 3
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.begin = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        config = TransactionDBOperationConfig(
            transaction=True,
            operations=[
                SingleDBOperationConfig(
                    type=DBOperationType.UPDATE,
                    instance="test_db",
                    mode=DBOperationMode.SQL,
                    sql="UPDATE orders SET status = 'PAID' WHERE id IN (:ids)",
                    batch_params={"ids": "ids"},
                ),
            ],
        )

        result = await executor._execute_transaction(
            config, context_manager, None, None
        )

        assert result.success is True
        assert result.rowcount == 3


class TestDBOperationExecutorExecute:
    """DBOperationExecutor.execute 单元测试"""

    @pytest.fixture
    def context_manager(self):
        ctx = Context("task_1")
        return ContextManager(ctx)

    @pytest.mark.asyncio
    async def test_execute_multiple_operations(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        operations = [
            SingleDBOperationConfig(
                type=DBOperationType.UPDATE,
                instance="test_db",
                table="orders",
                set={"status": "PAID"},
                where="id = 1",
            ),
            SingleDBOperationConfig(
                type=DBOperationType.DELETE,
                instance="test_db",
                table="logs",
                where="created_at < '2024-01-01'",
            ),
        ]

        result = await executor.execute(operations, context_manager, None, None)

        assert result.success is True
        assert len(result.results) == 2
        assert all(r.success for r in result.results)

    @pytest.mark.asyncio
    async def test_execute_stop_on_error_with_fail_on_error(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        # First call succeeds, second fails
        mock_conn.execute = AsyncMock(side_effect=[mock_result, Exception("Error")])
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        operations = [
            SingleDBOperationConfig(
                type=DBOperationType.UPDATE,
                instance="test_db",
                table="orders",
                set={"status": "PAID"},
                where="id = 1",
                fail_on_error=True,
            ),
            SingleDBOperationConfig(
                type=DBOperationType.DELETE,
                instance="test_db",
                table="logs",
                where="created_at < '2024-01-01'",
                fail_on_error=True,
            ),
            SingleDBOperationConfig(
                type=DBOperationType.DELETE,
                instance="test_db",
                table="other",
                where="1=1",
                fail_on_error=True,
            ),
        ]

        result = await executor.execute(operations, context_manager, None, None)

        assert result.success is False
        # Should stop after second operation fails
        assert len(result.results) == 2
        assert result.results[0].success is True
        assert result.results[1].success is False

    @pytest.mark.asyncio
    async def test_execute_continue_on_error_without_fail_on_error(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        # First call succeeds, second fails, third succeeds
        mock_conn.execute = AsyncMock(side_effect=[mock_result, Exception("Error"), mock_result])
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        operations = [
            SingleDBOperationConfig(
                type=DBOperationType.UPDATE,
                instance="test_db",
                table="orders",
                set={"status": "PAID"},
                where="id = 1",
                fail_on_error=False,  # Continue on error
            ),
            SingleDBOperationConfig(
                type=DBOperationType.DELETE,
                instance="test_db",
                table="logs",
                where="created_at < '2024-01-01'",
                fail_on_error=False,
            ),
            SingleDBOperationConfig(
                type=DBOperationType.DELETE,
                instance="test_db",
                table="other",
                where="1=1",
                fail_on_error=False,
            ),
        ]

        result = await executor.execute(operations, context_manager, None, None)

        assert result.success is False  # Overall success is False because one failed
        # Should continue to execute all operations
        assert len(result.results) == 3
        assert result.results[0].success is True
        assert result.results[1].success is False
        assert result.results[2].success is True

    @pytest.mark.asyncio
    async def test_execute_mixed_single_and_transaction(self, context_manager):
        pool_manager = Mock()
        engine = Mock()
        pool_manager.get_engine.return_value = engine

        mock_conn = Mock()
        mock_result = Mock()
        mock_result.rowcount = 1
        mock_conn.execute = AsyncMock(return_value=mock_result)
        mock_conn.commit = AsyncMock()
        mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_conn.__aexit__ = AsyncMock(return_value=None)
        engine.connect = Mock(return_value=mock_conn)
        engine.begin = Mock(return_value=mock_conn)

        executor = DBOperationExecutor(pool_manager)
        operations = [
            SingleDBOperationConfig(
                type=DBOperationType.UPDATE,
                instance="test_db",
                table="orders",
                set={"status": "PAID"},
                where="id = 1",
            ),
            TransactionDBOperationConfig(
                transaction=True,
                operations=[
                    SingleDBOperationConfig(
                        type=DBOperationType.UPDATE,
                        instance="test_db",
                        table="accounts",
                        set={"balance": "balance - 100"},
                        where="id = 1",
                    ),
                ],
            ),
        ]

        result = await executor.execute(operations, context_manager, None, None)

        assert result.success is True
        assert len(result.results) == 2
