import asyncio
from typing import Optional

from jsonpath_ng import parse

from tdc.config.models import ExecutionConfig, PipelineStepConfig, TaskConfig
from tdc.config.template_loader import TemplateLoader
from tdc.core.assertions import AssertionValidator
from tdc.core.execution_stats import ExecutionStats
from tdc.core.models import Context, ExecutionContext, PipelineResult
from tdc.pipeline.context import ContextManager
from tdc.core.db_assertions import DBAssertionValidator
from tdc.pipeline.gateway_auth import GatewayAuth
from tdc.pipeline.http_client import HTTPClient
from tdc.pipeline.user_provider import UserProvider
from tdc.storage.mysql_pool import MySQLPoolManager


class PipelineEngine:
    """管道执行引擎"""

    def __init__(
        self,
        template_loader: Optional[TemplateLoader] = None,
        pool_manager: Optional[MySQLPoolManager] = None,
        default_database: Optional[str] = None,
    ):
        self.http_client = HTTPClient()
        self.template_loader = template_loader
        self.pool_manager = pool_manager
        self.default_database = default_database

    async def execute(self, config: TaskConfig, ctx: Context) -> PipelineResult:
        """执行完整的管道（支持并发控制）"""
        execution_config = config.execution or ExecutionConfig()
        stats = ExecutionStats(total=execution_config.iterations)

        # 初始化用户提供者
        context_manager = ContextManager(ctx)
        user_provider = UserProvider(execution_config, context_manager)
        user_provider.initialize()

        # 创建并发控制信号量
        concurrency = max(1, execution_config.concurrency)
        semaphore = asyncio.Semaphore(concurrency)

        # 创建所有迭代任务
        tasks = []
        for i in range(execution_config.iterations):
            task = self._execute_iteration(
                i, execution_config, user_provider, config,
                ctx, context_manager, semaphore, stats
            )
            tasks.append(task)

        # 执行所有任务
        if concurrency == 1:
            # 串行执行
            for task in tasks:
                await task
        else:
            # 并发执行
            await asyncio.gather(*tasks, return_exceptions=True)

        # 构建结果
        success = stats.failed == 0 and stats.completed == stats.total
        return PipelineResult(
            context=ctx,
            success=success,
            error=None if success else f"Completed {stats.completed}/{stats.total}, failed: {stats.failed}",
            step_results=[stats.to_dict()]
        )

    async def _execute_iteration(
        self,
        iteration: int,
        execution_config: ExecutionConfig,
        user_provider: UserProvider,
        config: TaskConfig,
        ctx: Context,
        context_manager: ContextManager,
        semaphore: asyncio.Semaphore,
        stats: ExecutionStats
    ):
        """执行单次迭代（带并发控制）"""
        async with semaphore:
            try:
                # 获取当前用户
                user = user_provider.get_user(iteration)

                # 创建执行上下文
                execution = ExecutionContext(
                    iteration=iteration,
                    user=user,
                    total=execution_config.iterations
                )

                # 将 execution 存入 context 供后续使用（如 tag_mapping）
                # 使用固定键名，存储最后一次迭代的 execution
                ctx.set("_execution", execution)

                # 网关认证（如果配置）
                gateway_auth = None
                if config.gateway and self.template_loader:
                    gateway_auth = GatewayAuth(
                        config.gateway,
                        config.task_id,
                        self.template_loader,
                        context_manager
                    )
                    try:
                        await gateway_auth.authenticate(execution)
                    except Exception as e:
                        stats.add_result(iteration, False, f"gateway_auth: {e}")
                        if execution_config.fail_fast:
                            raise
                        return

                # 执行 pipeline steps
                iteration_results = await self._execute_pipeline(
                    config.pipeline, ctx, execution, gateway_auth, config.task_id
                )

                # 检查步骤结果
                all_success = all(r.get("success", True) for r in iteration_results)
                error_msg = None
                if not all_success:
                    failed_steps = [r for r in iteration_results if not r.get("success", True)]
                    error_msg = f"steps failed: {[s.get('step_id') for s in failed_steps]}"

                stats.add_result(iteration, all_success, error_msg)

                # 单次迭代延迟
                if iteration < execution_config.iterations - 1 and execution_config.delay_ms > 0:
                    await asyncio.sleep(execution_config.delay_ms / 1000)

            except Exception as e:
                stats.add_result(iteration, False, str(e))
                if execution_config.fail_fast:
                    raise

    async def _execute_pipeline(
        self,
        pipeline: list,
        ctx: Context,
        execution: ExecutionContext,
        gateway_auth: Optional[GatewayAuth],
        task_id: str
    ) -> list:
        """执行单次 pipeline，返回 step 结果列表"""
        step_results = []

        for step in pipeline:
            try:
                await self.execute_step(step, ctx, task_id, execution, gateway_auth)
                step_results.append({
                    "step_id": step.step_id,
                    "success": True
                })
            except Exception as e:
                step_results.append({
                    "step_id": step.step_id,
                    "success": False,
                    "error": str(e)
                })

        return step_results

    async def execute_step(
        self,
        step: PipelineStepConfig,
        ctx: Context,
        task_id: str,
        execution: Optional[ExecutionContext] = None,
        gateway_auth: Optional[GatewayAuth] = None
    ) -> dict:
        """执行单个步骤"""
        manager = ContextManager(ctx)

        # 检查条件
        if step.condition:
            if execution:
                condition_result = manager.render_template_with_execution(
                    step.condition, execution
                )
            else:
                condition_result = manager.render_template(step.condition)
            if not condition_result or condition_result.strip() in ("False", "None", ""):
                return {"skipped": True}

        # 加载并渲染请求体
        rendered_body = None
        if step.http.body_template:
            # 使用 template_loader 加载模板内容
            if self.template_loader:
                template_content = self.template_loader.load_body_template(
                    step.http.body_template, task_id
                )
            else:
                template_content = step.http.body_template

            # 渲染 Jinja2 模板（支持 execution 变量）
            if execution:
                rendered_body = manager.render_template_with_execution(
                    template_content, execution
                )
            else:
                rendered_body = manager.render_template(template_content)

        # 渲染 headers
        headers = manager.render_dict(step.http.headers)

        # 注入网关 token（如果配置了 gateway）
        if gateway_auth:
            headers = gateway_auth.apply_to_request(headers)

        # 更新 step.http.headers 以传递修改后的 headers
        step.http.headers = headers

        # 执行 HTTP 请求
        response = await self.http_client.request(step.http, rendered_body)

        # 【新增】执行粗粒度断言验证
        if step.assertions:
            assertion_result = AssertionValidator.validate(response, step.assertions)
            if not assertion_result.success:
                raise AssertionError(f"Step '{step.step_id}' assertion failed: {assertion_result.message}")

        # 提取字段到上下文
        if step.extract:
            response_data = response.json()
            for key, json_path in step.extract.items():
                value = self._extract_by_jsonpath(response_data, json_path)
                ctx.set(key, value)

        # 执行 DB 断言
        if step.db_assertions and self.pool_manager:
            for db_assertion in step.db_assertions:
                db_result = await DBAssertionValidator.validate(
                    config=db_assertion,
                    pool_manager=self.pool_manager,
                    context_manager=manager,
                    execution=execution,
                    default_database=self.default_database,
                )
                if not db_result.success and db_assertion.fail_on_error:
                    raise AssertionError(
                        f"Step '{step.step_id}' DB assertion failed: {db_result.message}"
                    )

        return {"status_code": response.status_code}

    def _extract_by_jsonpath(self, data: dict, path: str):
        """使用JSONPath提取数据"""
        jsonpath_expr = parse(path)
        matches = jsonpath_expr.find(data)
        if matches:
            return matches[0].value
        return None

    async def close(self):
        await self.http_client.close()
