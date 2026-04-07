import pytest
from pathlib import Path
from tdc.config.inheritance import InheritanceResolver
from tdc.core.exceptions import ConfigError


class TestInheritanceResolver:
    def test_no_inheritance(self, tmp_path):
        resolver = InheritanceResolver(tmp_path)
        config = {"task_id": "test", "execution": {"iterations": 10}}
        result = resolver.resolve(config)
        assert result["execution"]["iterations"] == 10

    def test_single_inheritance(self, tmp_path):
        # 创建基础配置
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        base_file = base_dir / "default.yaml"
        base_file.write_text("""
base_id: "default"
execution:
  iterations: 100
  delay_ms: 50
target_db:
  instance: "default_db"
""")

        resolver = InheritanceResolver(tmp_path)
        config = {
            "task_id": "test",
            "extends": "base/default",
            "execution": {"iterations": 10}  # 覆盖父配置
        }
        result = resolver.resolve(config)

        assert result["execution"]["iterations"] == 10  # 子配置覆盖
        assert result["execution"]["delay_ms"] == 50   # 继承父配置
        assert result["target_db"]["instance"] == "default_db"

    def test_multiple_inheritance(self, tmp_path):
        # 创建两个基础配置
        base_dir = tmp_path / "base"
        base_dir.mkdir()

        (base_dir / "db.yaml").write_text("""
base_id: "db"
target_db:
  instance: "biz_db"
  database: "test"
""")

        (base_dir / "exec.yaml").write_text("""
base_id: "exec"
execution:
  iterations: 50
  user_source: "faker"
""")

        resolver = InheritanceResolver(tmp_path)
        config = {
            "task_id": "test",
            "extends": ["base/db", "base/exec"],
            "execution": {"iterations": 10}
        }
        result = resolver.resolve(config)

        assert result["target_db"]["instance"] == "biz_db"
        assert result["execution"]["iterations"] == 10
        assert result["execution"]["user_source"] == "faker"

    def test_circular_inheritance_detection(self, tmp_path):
        base_dir = tmp_path / "base"
        base_dir.mkdir()

        (base_dir / "a.yaml").write_text("""
base_id: "a"
extends: "base/b"
execution:
  iterations: 1
""")

        (base_dir / "b.yaml").write_text("""
base_id: "b"
extends: "base/a"
execution:
  iterations: 2
""")

        resolver = InheritanceResolver(tmp_path)
        config = {"task_id": "test", "extends": "base/a"}

        with pytest.raises(ConfigError, match="Circular inheritance"):
            resolver.resolve(config)

    def test_nested_dict_merge(self, tmp_path):
        resolver = InheritanceResolver(tmp_path)
        base = {"target_db": {"instance": "db1", "database": "test"}}
        override = {"target_db": {"database": "prod"}}

        result = resolver._deep_merge(base, override)

        assert result["target_db"]["instance"] == "db1"  # 保留
        assert result["target_db"]["database"] == "prod"  # 覆盖

    def test_list_replace(self, tmp_path):
        """测试列表完全替换策略"""
        resolver = InheritanceResolver(tmp_path)
        base = {"tags": ["a", "b", "c"]}
        override = {"tags": ["x", "y"]}

        result = resolver._deep_merge(base, override)

        assert result["tags"] == ["x", "y"]  # 完全替换，不是合并

    def test_invalid_reference_format(self, tmp_path):
        """测试无效的引用格式"""
        resolver = InheritanceResolver(tmp_path)
        config = {"task_id": "test", "extends": "invalid_format"}

        with pytest.raises(ConfigError, match="Invalid base config reference"):
            resolver.resolve(config)

    def test_base_file_not_found(self, tmp_path):
        """测试基础配置文件不存在"""
        resolver = InheritanceResolver(tmp_path)
        config = {"task_id": "test", "extends": "base/nonexistent"}

        with pytest.raises(ConfigError, match="Base config file not found"):
            resolver.resolve(config)

    def test_base_config_caching(self, tmp_path):
        """测试基础配置缓存"""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        base_file = base_dir / "default.yaml"
        base_file.write_text("""
base_id: "default"
execution:
  iterations: 100
""")

        resolver = InheritanceResolver(tmp_path)

        # 第一次加载
        config1 = {"task_id": "test1", "extends": "base/default"}
        result1 = resolver.resolve(config1)

        # 第二次加载（应该从缓存读取）
        config2 = {"task_id": "test2", "extends": "base/default"}
        result2 = resolver.resolve(config2)

        assert result1["execution"]["iterations"] == 100
        assert result2["execution"]["iterations"] == 100
        assert "base/default" in resolver._base_cache

    def test_extends_removed_in_result(self, tmp_path):
        """测试继承元数据字段被移除"""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        (base_dir / "default.yaml").write_text("""
base_id: "default"
execution:
  iterations: 100
""")

        resolver = InheritanceResolver(tmp_path)
        config = {
            "task_id": "test",
            "extends": "base/default",
            "execution": {"iterations": 10}
        }
        result = resolver.resolve(config)

        assert "extends" not in result
        assert "base_id" not in result

    def test_deeply_nested_merge(self, tmp_path):
        """测试深层嵌套字典合并"""
        resolver = InheritanceResolver(tmp_path)
        base = {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "base",
                        "keep": "this"
                    }
                }
            }
        }
        override = {
            "level1": {
                "level2": {
                    "level3": {
                        "value": "override"
                    }
                }
            }
        }

        result = resolver._deep_merge(base, override)

        assert result["level1"]["level2"]["level3"]["value"] == "override"
        assert result["level1"]["level2"]["level3"]["keep"] == "this"
