import asyncio
from typing import Optional

from jsonpath_ng import parse

from tdc.config.models import ExecutionConfig, PipelineStepConfig, TaskConfig
from tdc.config.template_loader import TemplateLoader
from tdc.core.models import Context, ExecutionContext, PipelineResult
from tdc.pipeline.context import ContextManager
from tdc.pipeline.gateway_auth import GatewayAuth
from tdc.pipeline.http_client import HTTPClient
from tdc.pipeline.user_provider import UserProvider


class PipelineEngine:
    """管道执行引擎"""

    def __init__(self, template_loader: Optional[TemplateLoader] = None):
        self.http_client = HTTPClient()
        self.template_loader = template_loader

    async def execute(self, config: TaskConfig, ctx: Context) -> PipelineResult:
        """执行完整的管道"""
        execution_config = config.execution or ExecutionConfig()
        step_results = []

        # 初始化用户提供者
        context_manager = ContextManager(ctx)
        user_provider = UserProvider(execution_config, context_manager)
        user_provider.initialize()

        for i in range(execution_config.iterations):
            # 获取当前用户
            user = user_provider.get_user(i)

            # 创建执行上下文
            execution = ExecutionContext(
                iteration=i,
                user=user,
                total=execution_config.iterations
            )

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
                    step_results.append({
                        "iteration": i,
                        "step_id": "gateway_auth",
                        "success": False,
                        "error": str(e)
                    })
                    if config.on_failure.action == "stop":
                        return PipelineResult(
                            context=ctx,
                            success=False,
                            error=str(e),
                            step_results=step_results
                        )
                    continue  # on_failure == "continue" 时跳过本次迭代

            # 执行 pipeline steps
            iteration_results = await self._execute_pipeline(
                config.pipeline, ctx, execution, gateway_auth, config.task_id
            )
            step_results.extend(iteration_results)

            # 延迟（非最后一次）
            if i < execution_config.iterations - 1 and execution_config.delay_ms > 0:
                await asyncio.sleep(execution_config.delay_ms / 1000)

        return PipelineResult(
            context=ctx,
            success=all(r.get("success", True) for r in step_results),
            step_results=step_results
        )

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

        # 执行 HTTP 请求
        response = await self.http_client.request(step.http, rendered_body)

        # 提取字段到上下文
        if step.extract:
            response_data = response.json()
            for key, json_path in step.extract.items():
                value = self._extract_by_jsonpath(response_data, json_path)
                ctx.set(key, value)

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
