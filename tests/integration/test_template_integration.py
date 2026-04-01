import pytest
import json
from pathlib import Path
from tdc.config.template_loader import TemplateLoader
from tdc.pipeline.context import ContextManager
from tdc.core.models import Context


class TestTemplateIntegration:
    """模板加载与渲染的集成测试"""

    def test_full_template_load_and_render(self, tmp_path):
        """测试完整的模板加载和渲染流程"""
        # Arrange: 创建配置目录和模板文件
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        # 创建模板目录和文件
        templates_dir = config_dir / "templates" / "order_flow"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create_user.json"
        template_file.write_text('{"name": "{{ faker.name }}", "email": "{{ faker.email }}"}')

        # Act: 加载模板
        loader = TemplateLoader(str(config_dir))
        template_content = loader.load_body_template("create_user.json", "order_flow")

        # Act: 渲染模板
        ctx = Context(task_id="order_flow")
        manager = ContextManager(ctx)
        rendered = manager.render_template(template_content)

        # Assert: 验证渲染结果
        data = json.loads(rendered)
        assert "name" in data
        assert "email" in data
        assert data["name"] != "{{ faker.name }}"  # 应该被替换
        assert data["email"] != "{{ faker.email }}"  # 应该被替换

    def test_template_with_context_variables(self, tmp_path):
        """测试带上下文变量的模板"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        templates_dir = config_dir / "templates" / "order_flow"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create_order.json"
        template_file.write_text('{"user_id": "{{ context.get(\'user_id\') }}", "amount": 100}')

        # 加载并渲染
        loader = TemplateLoader(str(config_dir))
        template_content = loader.load_body_template("create_order.json", "order_flow")

        ctx = Context(task_id="order_flow")
        ctx.set("user_id", "user_12345")
        manager = ContextManager(ctx)
        rendered = manager.render_template(template_content)

        data = json.loads(rendered)
        assert data["user_id"] == "user_12345"
        assert data["amount"] == 100

    def test_inline_template_still_works(self, tmp_path):
        """测试内联模板仍然可用（向后兼容）"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))

        # 内联模板（不以 .json 结尾）
        inline = '{"name": "{{ faker.name }}"}'
        result = loader.load_body_template(inline, "test_task")

        # 应该原样返回
        assert result == inline

        # 渲染也应该正常工作
        ctx = Context(task_id="test_task")
        manager = ContextManager(ctx)
        rendered = manager.render_template(result)

        data = json.loads(rendered)
        assert "name" in data
        assert data["name"] != "{{ faker.name }}"

    def test_relative_path_template(self, tmp_path):
        """测试相对路径模板加载"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        # 创建嵌套目录结构
        orders_dir = config_dir / "templates" / "order_flow" / "orders"
        orders_dir.mkdir(parents=True)
        template_file = orders_dir / "create.json"
        template_file.write_text('{"order_no": "ORD{{ faker.random_number(digits=5) }}"}')

        loader = TemplateLoader(str(config_dir))
        template_content = loader.load_body_template("./orders/create.json", "order_flow")

        ctx = Context(task_id="order_flow")
        manager = ContextManager(ctx)
        rendered = manager.render_template(template_content)

        data = json.loads(rendered)
        assert "order_no" in data
        assert data["order_no"].startswith("ORD")
