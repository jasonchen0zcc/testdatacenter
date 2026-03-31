from jsonpath_ng import parse

from tdc.config.models import PipelineStepConfig, TaskConfig
from tdc.core.models import Context, PipelineResult
from tdc.pipeline.context import ContextManager
from tdc.pipeline.http_client import HTTPClient


class PipelineEngine:
    """管道执行引擎"""

    def __init__(self):
        self.http_client = HTTPClient()

    async def execute(self, config: TaskConfig, ctx: Context) -> PipelineResult:
        """执行完整的管道"""
        step_results = []

        for step in config.pipeline:
            try:
                await self.execute_step(step, ctx)
                step_results.append({"step_id": step.step_id, "success": True})
            except Exception as e:
                step_results.append({"step_id": step.step_id, "success": False, "error": str(e)})
                if config.on_failure.action == "stop":
                    return PipelineResult(context=ctx, success=False, error=str(e), step_results=step_results)

        return PipelineResult(context=ctx, success=True, step_results=step_results)

    async def execute_step(self, step: PipelineStepConfig, ctx: Context) -> dict:
        """执行单个步骤"""
        manager = ContextManager(ctx)

        # 检查条件
        if step.condition:
            condition_result = manager.render_template(step.condition)
            if not condition_result or condition_result.strip() in ("False", "None", ""):
                return {"skipped": True}

        # 渲染请求体
        rendered_body = None
        if step.http.body_template:
            rendered_body = manager.render_template(step.http.body_template)

        # 渲染headers中的模板
        headers = manager.render_dict(step.http.headers)

        # 执行HTTP请求
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
