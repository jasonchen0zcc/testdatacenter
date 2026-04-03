import asyncio
import logging
import re
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from jsonpath_ng import parse

from tdc.config.models import DBAssertionConfig, DBAssertionMode, RetryConfig
from tdc.core.assertions import AssertionResult
from tdc.pipeline.context import ContextManager
from tdc.storage.mysql_pool import MySQLPoolManager
from tdc.core.models import ExecutionContext

logger = logging.getLogger(__name__)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _validate_identifier(name: str, field: str) -> None:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid {field} identifier: {name}")


class DBAssertionValidator:
    """数据库断言验证器"""

    @staticmethod
    async def validate(
        config: DBAssertionConfig,
        pool_manager: MySQLPoolManager,
        context_manager: ContextManager,
        execution: Optional[ExecutionContext],
        default_database: Optional[str],
    ) -> AssertionResult:
        # 1. 延迟
        if config.delay_ms > 0:
            await asyncio.sleep(config.delay_ms / 1000)

        # 2. 渲染 SQL
        sql = _render_sql(config, context_manager, execution)
        database = config.database or default_database

        # 3. 获取引擎
        try:
            engine = pool_manager.get_engine(config.instance)
        except KeyError as e:
            return AssertionResult(success=False, message=str(e))

        # 4. 执行并断言（带重试）
        total_attempts = max(1, config.retry.max_attempts + 1)
        last_result: AssertionResult = AssertionResult(success=False, message="")

        for attempt in range(1, total_attempts + 1):
            try:
                result = await _execute_and_assert(sql, database, engine, config)
                return result
            except Exception as e:
                logger.warning(
                    "DB assertion attempt %s failed: %s",
                    attempt,
                    e,
                    exc_info=True,
                )
                last_result = AssertionResult(
                    success=False, message=f"{type(e).__name__}: {e}"
                )
                if attempt < total_attempts:
                    delay = _compute_retry_delay(config.retry, attempt)
                    await asyncio.sleep(delay)

        return last_result


def _render_sql(
    config: DBAssertionConfig,
    context_manager: ContextManager,
    execution: Optional[ExecutionContext],
) -> str:
    if config.mode == DBAssertionMode.SQL:
        template = config.sql or ""
        if execution:
            return context_manager.render_template_with_execution_and_context(
                template, execution, {}
            )
        return context_manager.render_template(template)

    # mode == TABLE
    where_clause = config.where or ""
    if execution:
        where_clause = context_manager.render_template_with_execution_and_context(
            where_clause, execution, {}
        )
    else:
        where_clause = context_manager.render_template(where_clause)
    return f"SELECT * FROM {config.table} WHERE {where_clause}"


async def _execute_and_assert(
    sql: str, database: Optional[str], engine: AsyncEngine, config: DBAssertionConfig
) -> AssertionResult:
    if database:
        _validate_identifier(database, "database")

    if config.mode == DBAssertionMode.TABLE and config.table:
        _validate_identifier(config.table, "table")

    async with engine.connect() as conn:
        if database:
            await conn.execute(text(f"USE `{database}`"))
        try:
            result_proxy = await asyncio.wait_for(
                conn.execute(text(sql)), timeout=config.timeout
            )
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(f"Query timed out after {config.timeout}s")
        rows = result_proxy.mappings().all()

    # expected_rows
    if config.expected_rows is not None:
        actual = len(rows)
        if actual != config.expected_rows:
            return AssertionResult(
                success=False,
                message=f"expected {config.expected_rows} rows but got {actual}",
            )

    # expected_value + query_path
    if config.expected_value is not None or config.query_path is not None:
        if not rows:
            return AssertionResult(
                success=False, message="no rows returned for value assertion"
            )
        first_row = dict(rows[0])
        if config.query_path:
            jsonpath_expr = parse(config.query_path)
            matches = jsonpath_expr.find(first_row)
            if not matches:
                return AssertionResult(
                    success=False,
                    message=f"query_path '{config.query_path}' not found in row",
                )
            actual_value = matches[0].value
        else:
            actual_value = list(first_row.values())[0] if first_row else None

        if actual_value != config.expected_value:
            return AssertionResult(
                success=False,
                message=f"expected value '{config.expected_value}' but got '{actual_value}'",
            )

    return AssertionResult(success=True)


def _compute_retry_delay(retry: RetryConfig, attempt: int) -> float:
    if retry.backoff == "exponential":
        return retry.delay * (2 ** (attempt - 1))
    return retry.delay
