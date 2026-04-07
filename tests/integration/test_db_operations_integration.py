"""DBOperation 集成测试"""

import pytest
import pytest_asyncio
from datetime import datetime

from tdc.config.models import (
    DBOperationMode,
    DBOperationType,
    SingleDBOperationConfig,
    TransactionDBOperationConfig,
)
from tdc.storage.db_operations import DBOperationExecutor
from tdc.storage.mysql_pool import MySQLPoolManager
from tdc.pipeline.context import ContextManager
from tdc.core.models import Context, ExecutionContext


@pytest_asyncio.fixture
async def pool_manager():
    """创建测试用的连接池管理器"""
    manager = MySQLPoolManager()
    # 使用测试数据库配置
    manager.add_instance(
        "test_db",
        host="localhost",
        port=3306,
        user="root",
        password="",
        database="test_tdc",
    )
    yield manager
    await manager.close_all()


@pytest_asyncio.fixture
async def setup_test_table(pool_manager):
    """创建测试表"""
    engine = pool_manager.get_engine("test_db")
    async with engine.begin() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_no VARCHAR(50),
                status VARCHAR(20),
                amount DECIMAL(10,2)
            )
        """)
        # 清理并插入测试数据
        await conn.execute("DELETE FROM test_orders")
        await conn.execute("""
            INSERT INTO test_orders (order_no, status, amount)
            VALUES ('ORD001', 'PENDING', 100.00),
                   ('ORD002', 'PENDING', 200.00),
                   ('ORD003', 'COMPLETED', 300.00)
        """)
    yield
    # 清理
    async with engine.begin() as conn:
        await conn.execute("DROP TABLE IF EXISTS test_orders")


@pytest.mark.asyncio
async def test_update_single_row(pool_manager, setup_test_table):
    """测试单条 UPDATE"""
    executor = DBOperationExecutor(pool_manager)
    ctx = Context("test_task")
    ctx.set("order_no", "ORD001")
    manager = ContextManager(ctx)

    config = SingleDBOperationConfig(
        type=DBOperationType.UPDATE,
        instance="test_db",
        database="test_tdc",
        table="test_orders",
        set={"status": "PAID"},
        where='order_no = "{{ context.get("order_no") }}"',
    )

    result = await executor._execute_single(
        config, manager, None, "test_tdc"
    )

    assert result.success
    assert result.rowcount == 1

    # 验证数据库状态
    engine = pool_manager.get_engine("test_db")
    async with engine.connect() as conn:
        await conn.execute("USE test_tdc")
        result_proxy = await conn.execute(
            "SELECT status FROM test_orders WHERE order_no = 'ORD001'"
        )
        row = result_proxy.fetchone()
        assert row[0] == "PAID"


@pytest.mark.asyncio
async def test_delete_with_condition(pool_manager, setup_test_table):
    """测试带条件的 DELETE"""
    executor = DBOperationExecutor(pool_manager)
    ctx = Context("test_task")
    manager = ContextManager(ctx)

    config = SingleDBOperationConfig(
        type=DBOperationType.DELETE,
        instance="test_db",
        database="test_tdc",
        table="test_orders",
        where="status = 'COMPLETED'",
    )

    result = await executor._execute_single(
        config, manager, None, "test_tdc"
    )

    assert result.success
    assert result.rowcount == 1

    # 验证数据库状态
    engine = pool_manager.get_engine("test_db")
    async with engine.connect() as conn:
        await conn.execute("USE test_tdc")
        result_proxy = await conn.execute(
            "SELECT COUNT(*) FROM test_orders WHERE status = 'COMPLETED'"
        )
        row = result_proxy.fetchone()
        assert row[0] == 0


@pytest.mark.asyncio
async def test_transaction_rollback_on_error(pool_manager, setup_test_table):
    """测试事务失败回滚"""
    executor = DBOperationExecutor(pool_manager)
    ctx = Context("test_task")
    manager = ContextManager(ctx)

    config = TransactionDBOperationConfig(
        transaction=True,
        fail_on_error=True,
        operations=[
            SingleDBOperationConfig(
                type=DBOperationType.UPDATE,
                instance="test_db",
                database="test_tdc",
                table="test_orders",
                set={"status": "PROCESSING"},
                where="order_no = 'ORD001'",
            ),
            # 这条会失败（表不存在）
            SingleDBOperationConfig(
                type=DBOperationType.UPDATE,
                instance="test_db",
                database="test_tdc",
                table="nonexistent_table",
                set={"x": 1},
                where="1=1",
            ),
        ],
    )

    result = await executor._execute_transaction(
        config, manager, None, "test_tdc"
    )

    assert not result.success

    # 验证第一条 UPDATE 被回滚
    engine = pool_manager.get_engine("test_db")
    async with engine.connect() as conn:
        await conn.execute("USE test_tdc")
        result_proxy = await conn.execute(
            "SELECT status FROM test_orders WHERE order_no = 'ORD001'"
        )
        row = result_proxy.fetchone()
        assert row[0] == "PENDING"  # 未被修改


@pytest.mark.asyncio
async def test_extract_rowcount(pool_manager, setup_test_table):
    """测试结果回写 rowcount"""
    executor = DBOperationExecutor(pool_manager)
    ctx = Context("test_task")
    manager = ContextManager(ctx)

    config = SingleDBOperationConfig(
        type=DBOperationType.UPDATE,
        instance="test_db",
        database="test_tdc",
        table="test_orders",
        set={"status": "ARCHIVED"},
        where="status = 'PENDING'",
        extract={"updated_count": "rowcount"},
    )

    result = await executor._execute_single(
        config, manager, None, "test_tdc"
    )

    assert result.success
    assert result.rowcount == 2  # ORD001, ORD002

    # 验证回写到 context
    assert ctx.get("updated_count") == 2


@pytest.mark.asyncio
async def test_sql_mode_update(pool_manager, setup_test_table):
    """测试 SQL 模式 UPDATE"""
    executor = DBOperationExecutor(pool_manager)
    ctx = Context("test_task")
    ctx.set("new_status", "SHIPPED")
    manager = ContextManager(ctx)

    config = SingleDBOperationConfig(
        type=DBOperationType.UPDATE,
        instance="test_db",
        database="test_tdc",
        mode=DBOperationMode.SQL,
        sql='UPDATE test_orders SET status = "{{ context.get("new_status") }}" WHERE order_no = "ORD003"',
    )

    result = await executor._execute_single(
        config, manager, None, "test_tdc"
    )

    assert result.success
    assert result.rowcount == 1

    # 验证
    engine = pool_manager.get_engine("test_db")
    async with engine.connect() as conn:
        await conn.execute("USE test_tdc")
        result_proxy = await conn.execute(
            "SELECT status FROM test_orders WHERE order_no = 'ORD003'"
        )
        row = result_proxy.fetchone()
        assert row[0] == "SHIPPED"
