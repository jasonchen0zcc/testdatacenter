import json
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from tdc.core.models import Context
from tdc.config.models import TagMappingConfig


class TagStore:
    """标记表存储操作"""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def save_tags(
        self,
        ctx: Context,
        tag_mapping: TagMappingConfig,
        database: str = None,
        task_log_id: int = None,
        table_name: str = "tdc_data_tag"
    ):
        """保存标记数据"""
        from jinja2 import Environment, BaseLoader
        from tdc.core.models import ExecutionContext
        env = Environment(loader=BaseLoader())

        # 获取 execution（如果存在）
        execution = ctx.get("_execution")

        # 渲染tag_mapping中的模板
        # 将 Context 对象转换为字典，使 template 能直接访问 context.orderNo
        context_dict = ctx.to_dict()

        def render_value(value):
            if isinstance(value, str) and value.startswith("{{"):
                template = env.from_string(value)
                render_ctx = {"context": context_dict, "now": datetime.now()}
                if execution:
                    render_ctx["execution"] = execution
                return template.render(**render_ctx)
            return value

        user_id = render_value(tag_mapping.user_id)
        order_id = render_value(tag_mapping.order_id)
        data_tag = render_value(tag_mapping.data_tag)

        ext_info = None
        if tag_mapping.ext_info:
            ext_info = json.dumps({
                k: render_value(v) for k, v in tag_mapping.ext_info.items()
            })

        # 使用完整表名（包含数据库名）
        full_table_name = f"{database}.{table_name}" if database else table_name

        # 构建 SQL，包含 task_log_id
        sql = text(f"""
            INSERT INTO {full_table_name}
            (task_log_id, user_id, order_id, data_tag, task_id, ext_info, created_at)
            VALUES (:task_log_id, :user_id, :order_id, :data_tag, :task_id, :ext_info, :created_at)
        """)

        await self.session.execute(sql, {
            "task_log_id": task_log_id,
            "user_id": user_id,
            "order_id": order_id,
            "data_tag": data_tag,
            "task_id": ctx.task_id,
            "ext_info": ext_info,
            "created_at": datetime.now()
        })
