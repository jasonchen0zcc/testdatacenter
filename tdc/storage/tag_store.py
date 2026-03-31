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
        table_name: str = "tdc_data_tag"
    ):
        """保存标记数据"""
        from jinja2 import Environment, BaseLoader
        env = Environment(loader=BaseLoader())

        # 渲染tag_mapping中的模板
        def render_value(value):
            if isinstance(value, str) and value.startswith("{{"):
                template = env.from_string(value)
                return template.render(context=ctx, now=datetime.now())
            return value

        user_id = render_value(tag_mapping.user_id)
        order_id = render_value(tag_mapping.order_id)
        data_tag = render_value(tag_mapping.data_tag)

        ext_info = None
        if tag_mapping.ext_info:
            ext_info = json.dumps({
                k: render_value(v) for k, v in tag_mapping.ext_info.items()
            })

        sql = text(f"""
            INSERT INTO {table_name} (user_id, order_id, data_tag, task_id, ext_info, created_at)
            VALUES (:user_id, :order_id, :data_tag, :task_id, :ext_info, :created_at)
        """)

        await self.session.execute(sql, {
            "user_id": user_id,
            "order_id": order_id,
            "data_tag": data_tag,
            "task_id": ctx.task_id,
            "ext_info": ext_info,
            "created_at": datetime.now()
        })
