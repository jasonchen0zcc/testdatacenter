import pytest
from unittest.mock import Mock

from tdc.core.models import Context, ExecutionContext
from tdc.pipeline.context import ContextManager


class TestContextManager:
    """测试 ContextManager"""

    def test_render_template_basic(self):
        """测试基本模板渲染"""
        context = Context("test_task")
        manager = ContextManager(context)

        result = manager.render_template("Hello {{ faker.name }}")
        assert "Hello" in result
        assert len(result) > 6

    def test_render_template_with_execution(self):
        """测试带 execution 变量的模板渲染"""
        context = Context("test_task")
        manager = ContextManager(context)

        execution = ExecutionContext(iteration=0, user="alice", total=10)
        result = manager.render_template_with_execution(
            "User: {{ execution.user }}, Iter: {{ execution.iteration }}",
            execution
        )

        assert result == "User: alice, Iter: 0"

    def test_render_template_with_execution_total(self):
        """测试 execution.total 变量"""
        context = Context("test_task")
        manager = ContextManager(context)

        execution = ExecutionContext(iteration=5, user="bob", total=100)
        result = manager.render_template_with_execution(
            "{{ execution.iteration + 1 }}/{{ execution.total }}",
            execution
        )

        assert result == "6/100"

    def test_render_dict_recursive(self):
        """测试递归渲染字典"""
        context = Context("test_task")
        context.set("name", "Test")
        manager = ContextManager(context)

        data = {
            "msg": 'Hello {{ context.get("name") }}',
            "nested": {
                "greeting": "Hi {{ faker.name }}"
            }
        }
        result = manager.render_dict(data)

        assert result["msg"] == "Hello Test"
        assert "Hi" in result["nested"]["greeting"]
