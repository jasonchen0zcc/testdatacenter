import asyncio
from typing import List
import structlog
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from tdc.config.loader import ConfigLoader
from tdc.config.models import TaskConfig
from tdc.storage.mysql_pool import MySQLPoolManager
from tdc.scheduler.router import TaskRouter

logger = structlog.get_logger()


class TDScheduler:
    """TDC调度器"""

    def __init__(self, config_dir: str):
        self.config_loader = ConfigLoader(config_dir)
        self.pool_manager = MySQLPoolManager()
        self.scheduler = AsyncIOScheduler()
        self.router: TaskRouter = None

    async def initialize(self):
        """初始化调度器"""
        db_config = self.config_loader.load_db_config()
        self.pool_manager.register_from_config(db_config)
        self.router = TaskRouter(self.pool_manager, self.config_loader)
        logger.info("scheduler_initialized")

    def load_tasks(self):
        """加载所有任务"""
        tasks = self.config_loader.load_task_configs()

        for task in tasks:
            if not task.enabled:
                logger.info("task_disabled", task_id=task.task_id)
                continue

            self._schedule_task(task)
            logger.info("task_scheduled", task_id=task.task_id, schedule=task.schedule)

    def _schedule_task(self, task: TaskConfig):
        """调度单个任务"""
        trigger = CronTrigger.from_crontab(task.schedule)

        # 计算超时时间（秒）
        timeout = task.timeout if task.timeout else 300

        self.scheduler.add_job(
            self._execute_task,
            trigger=trigger,
            id=task.task_id,
            name=task.task_name,
            args=[task],
            replace_existing=True,
            misfire_grace_time=300,
            max_instances=1,  # 防止同一任务并发执行
            coalesce=True,    # 错过多个执行点时合并为一次
        )

    async def _execute_task(self, task: TaskConfig):
        """执行任务包装器（带超时控制）"""
        logger.info("task_started", task_id=task.task_id)

        try:
            # 获取任务超时时间（默认5分钟）
            timeout = task.timeout if task.timeout else 300

            # 使用 asyncio.wait_for 实现超时控制
            result = await asyncio.wait_for(
                self.router.route(task),
                timeout=timeout
            )
            logger.info("task_completed", task_id=task.task_id, result=result)
        except asyncio.TimeoutError:
            logger.error("task_timeout", task_id=task.task_id, timeout=timeout)
        except Exception as e:
            logger.error("task_failed", task_id=task.task_id, error=str(e))

    def start(self):
        """启动调度器"""
        self.scheduler.start()
        logger.info("scheduler_started")

    def shutdown(self):
        """关闭调度器"""
        self.scheduler.shutdown()
        logger.info("scheduler_shutdown")

    async def run_task_now(self, task_id: str):
        """立即执行指定任务"""
        task = self.config_loader.load_task_by_id(task_id)
        return await self.router.route(task)
