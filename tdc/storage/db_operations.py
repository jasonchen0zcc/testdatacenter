import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tdc.config.models import (
    DBOperationItem,
    DBOperationMode,
    DBOperationTiming,
    DBOperationType,
    SingleDBOperationConfig,
    TransactionDBOperationConfig,
)
from tdc.core.models import Context, ExecutionContext
from tdc.pipeline.context import ContextManager
from tdc.storage.mysql_pool import MySQLPoolManager

logger = logging.getLogger(__name__)


@dataclass
class DBOperationResult:
    """数据库操作结果"""
    success: bool
    rowcount: int = 0
    message: Optional[str] = None
    results: List["DBOperationResult"] = field(default_factory=list)


class DBOperationExecutor:
    """数据库操作执行器，支持 UPDATE/DELETE"""

    def __init__(self, pool_manager: MySQLPoolManager):
        self.pool_manager = pool_manager

    async def execute(
        self,
        operations: List[DBOperationItem],
        context_manager: ContextManager,
        execution: Optional[ExecutionContext],
        default_database: Optional[str],
    ) -> DBOperationResult:
        """执行数据库操作列表"""
        results = []
        for op in operations:
            if isinstance(op, TransactionDBOperationConfig):
                result = await self._execute_transaction(
                    op, context_manager, execution, default_database
                )
            else:
                result = await self._execute_single(
                    op, context_manager, execution, default_database
                )
            results.append(result)
            if not result.success and (
                isinstance(op, TransactionDBOperationConfig) and op.fail_on_error
                or isinstance(op, SingleDBOperationConfig) and op.fail_on_error
            ):
                break
        return DBOperationResult(
            success=all(r.success for r in results),
            results=results,
        )

    def _build_sql(self, config: SingleDBOperationConfig) -> str:
        """根据配置构建 SQL"""
        if config.mode == DBOperationMode.SQL:
            return config.sql or ""

        # table 模式
        if config.type == DBOperationType.UPDATE:
            set_clause = ", ".join([f"`{k}` = :{k}" for k in config.set.keys()])
            return f"UPDATE `{config.table}` SET {set_clause} WHERE {config.where}"
        else:  # DELETE
            return f"DELETE FROM `{config.table}` WHERE {config.where}"

    def _render_sql(
        self,
        config: SingleDBOperationConfig,
        context_manager: ContextManager,
        execution: Optional[ExecutionContext],
    ) -> str:
        """渲染 SQL 模板"""
        sql = self._build_sql(config)
        if execution:
            return context_manager.render_template_with_execution(sql, execution)
        return context_manager.render_template(sql)

    def _expand_batch_params(
        self,
        sql: str,
        params: Dict[str, Any],
        batch_params: Dict[str, str],
        context: Context,
    ) -> tuple[str, Dict[str, Any]]:
        """展开批量参数"""
        for param_name, context_key in batch_params.items():
            values = context.get(context_key, [])
            if not isinstance(values, list):
                values = [values]
            placeholders = [f":{param_name}_{i}" for i in range(len(values))]
            sql = sql.replace(f":{param_name}", ", ".join(placeholders))
            for i, v in enumerate(values):
                params[f"{param_name}_{i}"] = v
        return sql, params

    async def _execute_single(
        self,
        config: SingleDBOperationConfig,
        context_manager: ContextManager,
        execution: Optional[ExecutionContext],
        default_database: Optional[str],
    ) -> DBOperationResult:
        """执行单条数据库操作"""
        try:
            sql = self._render_sql(config, context_manager, execution)
            database = config.database or default_database

            engine = self.pool_manager.get_engine(config.instance)

            params = config.params or {}
            if config.batch_params:
                sql, params = self._expand_batch_params(
                    sql, params, config.batch_params, context_manager.context
                )

            async with engine.connect() as conn:
                if database:
                    await conn.execute(text(f"USE `{database}`"))

                result = await conn.execute(text(sql), params)
                await conn.commit()

                rowcount = result.rowcount

                if config.extract:
                    for ctx_key, result_type in config.extract.items():
                        if result_type == "rowcount":
                            context_manager.context.set(ctx_key, rowcount)

                return DBOperationResult(success=True, rowcount=rowcount)

        except Exception as e:
            logger.error("DB operation failed: %s", e, exc_info=True)
            return DBOperationResult(
                success=False,
                message=f"{type(e).__name__}: {e}",
            )

    async def _execute_transaction(
        self,
        config: TransactionDBOperationConfig,
        context_manager: ContextManager,
        execution: Optional[ExecutionContext],
        default_database: Optional[str],
    ) -> DBOperationResult:
        """执行事务包裹的操作"""
        if not config.operations:
            return DBOperationResult(success=True)

        try:
            first_op = config.operations[0]
            engine = self.pool_manager.get_engine(first_op.instance)

            total_rowcount = 0

            async with engine.begin() as conn:  # 自动事务
                database = first_op.database or default_database
                if database:
                    await conn.execute(text(f"USE `{database}`"))

                for op in config.operations:
                    sql = self._render_sql(op, context_manager, execution)
                    params = op.params or {}
                    if op.batch_params:
                        sql, params = self._expand_batch_params(
                            sql, params, op.batch_params, context_manager.context
                        )
                    result = await conn.execute(text(sql), params)
                    total_rowcount += result.rowcount

            return DBOperationResult(success=True, rowcount=total_rowcount)

        except Exception as e:
            logger.error("Transaction failed: %s", e, exc_info=True)
            return DBOperationResult(
                success=False,
                message=f"Transaction failed: {type(e).__name__}: {e}",
            )
