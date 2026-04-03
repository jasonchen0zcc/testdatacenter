import structlog

from tdc.config.models import TaskConfig
from tdc.config.template_loader import TemplateLoader
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
        self.template_loader = TemplateLoader(str(config_loader.config_dir))

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
        engine = PipelineEngine(
            self.template_loader,
            pool_manager=self.pool_manager,
            default_database=config.target_db.database,
        )
        ctx = Context(task_id=config.task_id)

        session_maker = self.pool_manager.get_session_maker(config.target_db.instance)
        async with session_maker() as session:
            async with session.begin():
                inserter = BatchInserter(session, config.target_db.database, log_database="tdc")

                # 检查是否有运行中的任务（分布式锁）
                if await inserter.task_logger.is_task_running(config.task_id):
                    logger.warning("task_already_running", task_id=config.task_id)
                    return {"skipped": True, "reason": "task_already_running"}

                # 记录任务开始
                iterations = config.execution.iterations if config.execution else 1
                log_id = await inserter.task_logger.start_task(
                    task_id=config.task_id,
                    task_name=config.task_name,
                    task_type=config.task_type.value,
                    total_count=iterations
                )

                try:
                    result = await engine.execute(config, ctx)

                    # 计算成功/失败数量
                    step_results = result.step_results if hasattr(result, 'step_results') else []
                    success_count = sum(1 for r in step_results if r.get('success', False))
                    failed_count = len(step_results) - success_count

                    # 保存标记（传入 task_log_id 建立关联）
                    if result.success and config.tag_mapping:
                        await inserter.tag_store.save_tags(
                            ctx, config.tag_mapping, "tdc", task_log_id=log_id
                        )

                    # 记录任务完成
                    await inserter.task_logger.complete_task(
                        success_count=success_count,
                        failed_count=failed_count,
                        error_msg=result.error if hasattr(result, 'error') else None
                    )

                    return result
                except Exception as e:
                    # 记录任务失败
                    await inserter.task_logger.complete_task(
                        success_count=0,
                        failed_count=iterations,
                        error_msg=str(e)[:500]
                    )
                    raise
                finally:
                    await engine.close()

    async def _execute_direct_insert(self, config: TaskConfig):
        """执行直接插入任务"""
        generator = DataGeneratorEngine(config.data_template)
        records = generator.generate_all()

        session_maker = self.pool_manager.get_session_maker(config.target_db.instance)
        async with session_maker() as session:
            async with session.begin():
                inserter = BatchInserter(session, config.target_db.database, log_database="tdc")
                ctx = Context(task_id=config.task_id)

                # 记录任务开始
                log_id = await inserter.task_logger.start_task(
                    task_id=config.task_id,
                    task_name=config.task_name,
                    task_type=config.task_type.value,
                    total_count=len(records)
                )

                try:
                    # 将 task_log_id 存入 context，供 insert_records 使用
                    ctx.set("_task_log_id", log_id)
                    await inserter.insert_records(
                        config.data_template.table,
                        records,
                        ctx,
                        config.tag_mapping,
                        task_log_id=log_id
                    )

                    # 记录任务完成
                    await inserter.task_logger.complete_task(
                        success_count=len(records),
                        failed_count=0
                    )

                    return {"success": True, "records_count": len(records)}
                except Exception as e:
                    # 记录任务失败
                    await inserter.task_logger.complete_task(
                        success_count=0,
                        failed_count=len(records),
                        error_msg=str(e)[:500]
                    )
                    raise
