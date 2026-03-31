import structlog

from tdc.config.models import TaskConfig
from tdc.core.constants import TaskType
from tdc.core.models import Context
from tdc.pipeline.engine import PipelineEngine
from tdc.generator.engine import DataGeneratorEngine
from tdc.storage.mysql_pool import MySQLPoolManager
from tdc.storage.batch_insert import BatchInserter

logger = structlog.get_logger()


class TaskRouter:
    """任务路由器 - 根据任务类型分发到不同的执行器"""

    def __init__(self, pool_manager: MySQLPoolManager, config_loader):
        self.pool_manager = pool_manager
        self.config_loader = config_loader

    async def route(self, task_config: TaskConfig):
        """路由任务到对应的执行器"""
        logger.info("routing_task", task_id=task_config.task_id, task_type=task_config.task_type.value)

        if task_config.task_type == TaskType.HTTP_SOURCE:
            return await self._execute_http_source(task_config)
        elif task_config.task_type == TaskType.DIRECT_INSERT:
            return await self._execute_direct_insert(task_config)
        else:
            raise ValueError(f"Unknown task type: {task_config.task_type}")

    async def _execute_http_source(self, config: TaskConfig):
        """执行HTTP源任务"""
        engine = PipelineEngine()
        ctx = Context(task_id=config.task_id)

        try:
            result = await engine.execute(config, ctx)

            # 保存标记
            if result.success and config.tag_mapping:
                session_maker = self.pool_manager.get_session_maker(config.target_db.instance)
                async with session_maker() as session:
                    async with session.begin():
                        inserter = BatchInserter(session)
                        await inserter.tag_store.save_tags(ctx, config.tag_mapping)

            return result
        finally:
            await engine.close()

    async def _execute_direct_insert(self, config: TaskConfig):
        """执行直接插入任务"""
        generator = DataGeneratorEngine(config.data_template)
        records = generator.generate_all()

        session_maker = self.pool_manager.get_session_maker(config.target_db.instance)
        async with session_maker() as session:
            async with session.begin():
                inserter = BatchInserter(session)
                ctx = Context(task_id=config.task_id)

                await inserter.insert_records(
                    config.data_template.table,
                    records,
                    ctx,
                    config.tag_mapping
                )

        return {"success": True, "records_count": len(records)}
