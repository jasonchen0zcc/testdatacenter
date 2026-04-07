import os
import pytest
from pathlib import Path
from unittest.mock import patch

from tdc.config.secrets import SecretResolver
from tdc.config.models import SecretRef
from tdc.core.exceptions import ConfigError


class TestSecretResolver:
    """密钥解析器测试"""

    @pytest.fixture
    def resolver(self):
        return SecretResolver()

    @pytest.fixture
    def temp_secret_file(self, tmp_path):
        secret_file = tmp_path / "secret.txt"
        secret_file.write_text("my_secret_value")
        return str(secret_file)

    def test_resolve_env_provider_success(self, resolver):
        """测试 env provider 成功解析"""
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            config = {"password": {"provider": "env", "key": "TEST_VAR"}}
            result = resolver.resolve_config(config)
            assert result["password"] == "test_value"

    def test_resolve_env_provider_with_default(self, resolver):
        """测试 env provider 使用默认值"""
        config = {"password": {"provider": "env", "key": "NONEXISTENT_VAR", "default": "default_pass"}}
        result = resolver.resolve_config(config)
        assert result["password"] == "default_pass"

    def test_resolve_env_provider_missing_key(self, resolver):
        """测试 env provider 缺少 key 字段"""
        config = {"password": {"provider": "env"}}
        with pytest.raises(ConfigError, match="env provider requires 'key' field"):
            resolver.resolve_config(config)

    def test_resolve_env_provider_not_found_no_default(self, resolver):
        """测试 env provider 变量不存在且无默认值"""
        config = {"password": {"provider": "env", "key": "NONEXISTENT_VAR"}}
        with pytest.raises(ConfigError, match="Environment variable not found"):
            resolver.resolve_config(config)

    def test_resolve_file_provider_success(self, resolver, temp_secret_file):
        """测试 file provider 成功解析"""
        config = {"password": {"provider": "file", "path": temp_secret_file}}
        result = resolver.resolve_config(config)
        assert result["password"] == "my_secret_value"

    def test_resolve_file_provider_missing_path(self, resolver):
        """测试 file provider 缺少 path 字段"""
        config = {"password": {"provider": "file"}}
        with pytest.raises(ConfigError, match="file provider requires 'path' field"):
            resolver.resolve_config(config)

    def test_resolve_file_provider_not_found(self, resolver):
        """测试 file provider 文件不存在"""
        config = {"password": {"provider": "file", "path": "/nonexistent/secret.txt"}}
        with pytest.raises(ConfigError, match="Secret file not found"):
            resolver.resolve_config(config)

    def test_resolve_string_env_substitution(self, resolver):
        """测试字符串中的环境变量替换"""
        with patch.dict(os.environ, {"DB_HOST": "localhost", "DB_PORT": "3306"}):
            config = {"url": "mysql://${DB_HOST}:${DB_PORT}/mydb"}
            result = resolver.resolve_config(config)
            assert result["url"] == "mysql://localhost:3306/mydb"

    def test_resolve_string_env_with_default(self, resolver):
        """测试字符串中的环境变量带默认值"""
        config = {"password": "${NONEXISTENT:-default_password}"}
        result = resolver.resolve_config(config)
        assert result["password"] == "default_password"

    def test_resolve_string_env_no_substitution(self, resolver):
        """测试字符串中不存在的环境变量保持原样"""
        config = {"password": "${UNDEFINED_VAR}"}
        result = resolver.resolve_config(config)
        assert result["password"] == "${UNDEFINED_VAR}"

    def test_resolve_nested_config(self, resolver):
        """测试嵌套配置解析"""
        with patch.dict(os.environ, {"API_KEY": "secret123"}):
            config = {
                "database": {
                    "host": "localhost",
                    "password": {"provider": "env", "key": "API_KEY"}
                },
                "api": {
                    "url": "https://api.${DOMAIN:-example.com}"
                }
            }
            result = resolver.resolve_config(config)
            assert result["database"]["host"] == "localhost"
            assert result["database"]["password"] == "secret123"
            assert result["api"]["url"] == "https://api.example.com"

    def test_resolve_list_config(self, resolver):
        """测试列表配置解析"""
        with patch.dict(os.environ, {"KEY1": "value1"}):
            config = {
                "items": [
                    {"provider": "env", "key": "KEY1"},
                    "static_value",
                    "${KEY2:-value2}"
                ]
            }
            result = resolver.resolve_config(config)
            assert result["items"] == ["value1", "static_value", "value2"]

    def test_resolve_caching(self, resolver):
        """测试密钥解析缓存"""
        with patch.dict(os.environ, {"CACHED_VAR": "cached_value"}):
            ref = SecretRef(provider="env", key="CACHED_VAR")

            # 第一次解析
            result1 = resolver._resolve_secret(ref)
            assert result1 == "cached_value"

            # 修改环境变量（模拟外部变化）
            with patch.dict(os.environ, {"CACHED_VAR": "new_value"}):
                # 第二次解析应该返回缓存值
                result2 = resolver._resolve_secret(ref)
                assert result2 == "cached_value"  # 缓存值

    def test_resolve_unknown_provider_ignored(self, resolver):
        """测试未知 provider 被忽略（当作普通字典处理）"""
        config = {"password": {"provider": "unknown", "key": "test"}}
        result = resolver.resolve_config(config)
        # 未知 provider 被当作普通字典处理，不进行解析
        assert result["password"]["provider"] == "unknown"
        assert result["password"]["key"] == "test"

    def test_resolve_vault_not_implemented(self, resolver):
        """测试 vault provider 未实现"""
        config = {"password": {"provider": "vault", "path": "secret/data/mydb"}}
        with pytest.raises(ConfigError, match="Vault provider not yet implemented"):
            resolver.resolve_config(config)

    def test_resolve_aws_sm_not_implemented(self, resolver):
        """测试 aws_sm provider 未实现"""
        config = {"password": {"provider": "aws_sm", "key": "my-secret"}}
        with pytest.raises(ConfigError, match="AWS Secrets Manager provider not yet implemented"):
            resolver.resolve_config(config)

    def test_resolve_non_dict_with_provider_field(self, resolver):
        """测试带有 provider 字段但不是 SecretRef 的字典"""
        config = {"provider": "some_value", "other": "data"}
        result = resolver.resolve_config(config)
        assert result["provider"] == "some_value"
        assert result["other"] == "data"

    def test_resolve_preserve_non_string_values(self, resolver):
        """测试保留非字符串值"""
        config = {
            "count": 42,
            "enabled": True,
            "ratio": 3.14,
            "empty": None
        }
        result = resolver.resolve_config(config)
        assert result["count"] == 42
        assert result["enabled"] is True
        assert result["ratio"] == 3.14
        assert result["empty"] is None
