from typing import List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from tdc.storage.tag_store import TagStore
from tdc.storage.task_log import TaskLogger
from tdc.core.models import Context
from tdc.config.models import TagMappingConfig


class BatchInserter:
    """批量插入器"""

    def __init__(self, session: AsyncSession, database: str = "tdc", log_database: str = "tdc"):
        self.session = session
        self.database = database
        self.log_database = log_database
        self.tag_store = TagStore(session)
        self.task_logger = TaskLogger(session, log_database)

    async def insert_records(
        self,
        table: str,
        records: List[Dict[str, Any]],
        ctx: Context = None,
        tag_mapping: TagMappingConfig = None,
        task_log_id: int = None
    ):
        """批量插入记录并保存标记"""
        if not records:
            return

        # 构建INSERT语句
        columns = list(records[0].keys())
        placeholders = ", ".join([f":{col}" for col in columns])
        column_str = ", ".join(columns)

        full_table = f"{self.database}.{table}" if self.database else table
        sql = f"INSERT INTO {full_table} ({column_str}) VALUES ({placeholders})"

        # 批量执行
        for record in records:
            await self.session.execute(text(sql), record)

        # 保存标记（传入 task_log_id）
        if ctx and tag_mapping:
            await self.tag_store.save_tags(ctx, tag_mapping, self.log_database, task_log_id=task_log_id)
