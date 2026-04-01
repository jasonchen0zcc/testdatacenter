import pytest
from pathlib import Path
from tdc.config.template_loader import TemplateLoader


class TestTemplateLoader:
    def test_load_shorthand_filename(self, tmp_path):
        """测试简写形式：纯文件名自动解析为 templates/{task_id}/{filename}"""
        # Arrange
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        templates_dir = config_dir / "templates" / "test_task"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create_user.json"
        template_file.write_text('{"name": "test"}')

        loader = TemplateLoader(str(config_dir))

        # Act
        result = loader.load_body_template("create_user.json", "test_task")

        # Assert
        assert result == '{"name": "test"}'

    def test_load_relative_path(self, tmp_path):
        """测试相对路径：./subdir/file.json"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        templates_dir = config_dir / "templates" / "test_task" / "orders"
        templates_dir.mkdir(parents=True)
        template_file = templates_dir / "create.json"
        template_file.write_text('{"order": true}')

        loader = TemplateLoader(str(config_dir))
        result = loader.load_body_template("./orders/create.json", "test_task")

        assert result == '{"order": true}'

    def test_load_absolute_path(self, tmp_path):
        """测试完整路径：templates/other/shared.json"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        shared_dir = config_dir / "templates" / "shared"
        shared_dir.mkdir(parents=True)
        template_file = shared_dir / "common.json"
        template_file.write_text('{"shared": true}')

        loader = TemplateLoader(str(config_dir))
        result = loader.load_body_template("templates/shared/common.json", "test_task")

        assert result == '{"shared": true}'

    def test_load_inline_template(self, tmp_path):
        """测试内联模板：不以 .json 结尾的直接返回原字符串"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))
        inline = '{"inline": "{{ faker.name }}"}'
        result = loader.load_body_template(inline, "test_task")

        assert result == inline

    def test_load_inline_json_like_but_not_file(self, tmp_path):
        """测试看起来像JSON路径但文件不存在时返回原字符串"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))
        inline = '{"not": "a file"}'
        result = loader.load_body_template(inline, "test_task")

        assert result == inline

    def test_file_not_found_raises_error(self, tmp_path):
        """测试文件不存在且以 .json 结尾时抛出 FileNotFoundError"""
        config_dir = tmp_path / "configs"
        config_dir.mkdir()

        loader = TemplateLoader(str(config_dir))

        with pytest.raises(FileNotFoundError) as exc_info:
            loader.load_body_template("nonexistent.json", "test_task")

        assert "nonexistent.json" in str(exc_info.value)
