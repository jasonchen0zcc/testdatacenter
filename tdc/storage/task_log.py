"""Task execution log storage."""
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class TaskLogger:
    """Records task execution logs to database."""

    def __init__(self, session: AsyncSession, database: str = "tdc"):
        self.session = session
        self.database = database
        self.log_id: Optional[int] = None

    async def is_task_running(self, task_id: str) -> bool:
        """Check if a task is currently running (distributed lock)."""
        table_name = f"{self.database}.tdc_task_log"

        result = await self.session.execute(
            text(f"""
                SELECT COUNT(*) FROM {table_name}
                WHERE task_id = :task_id AND status = 'running'
            """),
            {"task_id": task_id}
        )
        count = result.scalar()
        return count > 0

    async def start_task(
        self,
        task_id: str,
        task_name: str,
        task_type: str,
        total_count: int = 0
    ) -> int:
        """Record task start and return log ID."""
        table_name = f"{self.database}.tdc_task_log"

        result = await self.session.execute(
            text(f"""
                INSERT INTO {table_name}
                (task_id, task_name, task_type, status, total_count, started_at)
                VALUES (:task_id, :task_name, :task_type, :status, :total_count, :started_at)
            """),
            {
                "task_id": task_id,
                "task_name": task_name,
                "task_type": task_type,
                "status": "running",
                "total_count": total_count,
                "started_at": datetime.now()
            }
        )
        await self.session.flush()

        # Get the inserted ID
        result = await self.session.execute(text("SELECT LAST_INSERT_ID()"))
        self.log_id = result.scalar()
        return self.log_id

    async def complete_task(
        self,
        success_count: int = 0,
        failed_count: int = 0,
        error_msg: Optional[str] = None
    ):
        """Update task as completed."""
        if self.log_id is None:
            return

        total = success_count + failed_count
        status = "success" if failed_count == 0 else "partial" if success_count > 0 else "failed"

        table_name = f"{self.database}.tdc_task_log"
        await self.session.execute(
            text(f"""
                UPDATE {table_name}
                SET status = :status,
                    success_count = :success_count,
                    failed_count = :failed_count,
                    error_msg = :error_msg,
                    finished_at = :finished_at
                WHERE id = :log_id
            """),
            {
                "log_id": self.log_id,
                "status": status,
                "success_count": success_count,
                "failed_count": failed_count,
                "error_msg": error_msg,
                "finished_at": datetime.now()
            }
        )
