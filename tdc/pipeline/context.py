import random
from datetime import datetime
from typing import Any
from jinja2 import Environment, BaseLoader
from faker import Faker

from tdc.core.models import Context


class ContextManager:
    """上下文管理器，支持Jinja2模板渲染"""

    def __init__(self, context: Context, locale: str = "zh_CN"):
        self.context = context
        self.faker = Faker(locale)
        self.env = Environment(loader=BaseLoader())
        self._register_filters()

    def _register_filters(self):
        """注册自定义过滤器"""
        self.env.filters["format_date"] = lambda d, fmt: d.strftime(fmt) if isinstance(d, datetime) else d
        self.env.filters["iso"] = lambda d: d.isoformat() if isinstance(d, datetime) else d

    def render_template(self, template_str: str) -> str:
        """渲染模板字符串"""
        template = self.env.from_string(template_str)
        return template.render(
            context=self.context,
            faker=self.faker,
            now=datetime.now()
        )

    def render_dict(self, data: dict) -> dict:
        """递归渲染字典中的模板"""
        result = {}
        for key, value in data.items():
            if isinstance(value, str):
                result[key] = self.render_template(value)
            elif isinstance(value, dict):
                result[key] = self.render_dict(value)
            else:
                result[key] = value
        return result
