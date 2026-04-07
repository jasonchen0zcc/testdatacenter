import pytest
from pydantic import ValidationError

from tdc.config.models import (
    SecretRef,
    TaskConfig,
)


class TestSecretRef:
    """测试 SecretRef 模型"""

    def test_secret_ref_with_env_provider(self):
        """测试 env 提供者的 SecretRef"""
        secret = SecretRef(provider="env", key="DB_PASSWORD")
        assert secret.provider == "env"
        assert secret.key == "DB_PASSWORD"
        assert secret.encoding == "utf-8"  # default value

    def test_secret_ref_with_vault_provider(self):
        """测试 vault 提供者的 SecretRef"""
        secret = SecretRef(
            provider="vault",
            path="secret/data/db",
            key="password",
            default="fallback_password"
        )
        assert secret.provider == "vault"
        assert secret.path == "secret/data/db"
        assert secret.key == "password"
        assert secret.default == "fallback_password"

    def test_secret_ref_with_file_provider(self):
        """测试 file 提供者的 SecretRef"""
        secret = SecretRef(
            provider="file",
            path="/secrets/password.txt",
            encoding="utf-16"
        )
        assert secret.provider == "file"
        assert secret.path == "/secrets/password.txt"
        assert secret.encoding == "utf-16"

    def test_secret_ref_with_aws_sm_provider(self):
        """测试 aws_sm 提供者的 SecretRef"""
        secret = SecretRef(
            provider="aws_sm",
            secret_id="my-secret-id",
            key="api_key"
        )
        assert secret.provider == "aws_sm"
        assert secret.secret_id == "my-secret-id"
        assert secret.key == "api_key"

    def test_secret_ref_provider_required(self):
        """测试 provider 字段是必需的"""
        with pytest.raises(ValidationError) as exc_info:
            SecretRef()
        assert "provider" in str(exc_info.value)


class TestTaskConfigInheritance:
    """测试 TaskConfig 继承字段"""

    def test_task_config_with_string_extends(self):
        """测试 extends 为字符串的情况"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "extends": "base_config",
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.extends == "base_config"

    def test_task_config_with_list_extends(self):
        """测试 extends 为列表的情况"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "extends": ["base_config", "common_settings"],
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.extends == ["base_config", "common_settings"]

    def test_task_config_extends_empty_list_invalid(self):
        """测试 extends 为空列表应该报错"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "extends": [],
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        with pytest.raises(ValidationError) as exc_info:
            TaskConfig(**data)
        assert "extends list cannot be empty" in str(exc_info.value)

    def test_task_config_without_extends(self):
        """测试没有 extends 的情况"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.extends is None

    def test_task_config_with_imports(self):
        """测试 imports 字段"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "imports": {
                "common": "configs/common.yaml",
                "secrets": "configs/secrets.yaml"
            },
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.imports == {
            "common": "configs/common.yaml",
            "secrets": "configs/secrets.yaml"
        }

    def test_task_config_with_metadata(self):
        """测试元数据字段（category, tags, owner）"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "category": "data_generation",
            "tags": ["critical", "nightly"],
            "owner": "team-platform",
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.category == "data_generation"
        assert config.tags == ["critical", "nightly"]
        assert config.owner == "team-platform"

    def test_task_config_default_tags(self):
        """测试 tags 默认为空列表"""
        data = {
            "task_id": "test_task",
            "task_name": "Test Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.tags == []
