import os
import re
from typing import Dict, Any, Optional
from pathlib import Path

from tdc.config.models import SecretRef
from tdc.core.exceptions import ConfigError


class SecretResolver:
    """密钥解析器，支持多种密钥管理方式"""

    ENV_PATTERN = re.compile(r'\$\{([^}]+)\}')
    VALID_PROVIDERS = {"env", "file", "vault", "aws_sm"}

    def __init__(self):
        self._cache: Dict[str, str] = {}

    def _is_valid_secret_ref(self, value: dict) -> bool:
        """检查字典是否是有效的 SecretRef"""
        provider = value.get("provider")
        return isinstance(provider, str) and provider in self.VALID_PROVIDERS

    def resolve_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """递归解析配置中的所有密钥引用"""
        return self._resolve_value(config)

    def _resolve_value(self, value: Any) -> Any:
        """递归解析值"""
        if isinstance(value, dict):
            if "provider" in value and self._is_valid_secret_ref(value):
                try:
                    ref = SecretRef(**value)
                    return self._resolve_secret(ref)
                except (ValueError, TypeError):
                    pass
            return {k: self._resolve_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._resolve_value(item) for item in value]
        elif isinstance(value, str):
            return self._resolve_string(value)
        return value

    def _resolve_string(self, value: str) -> str:
        """解析字符串中的环境变量引用"""
        def replace(match):
            var_expr = match.group(1)
            if ':-' in var_expr:
                var_name, default = var_expr.split(':-', 1)
                return os.environ.get(var_name, default)
            else:
                return os.environ.get(var_expr, match.group(0))
        return self.ENV_PATTERN.sub(replace, value)

    def _resolve_secret(self, ref: SecretRef) -> str:
        """解析密钥引用"""
        cache_key = f"{ref.provider}:{ref.path or ''}:{ref.key or ''}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        if ref.provider == "env":
            result = self._resolve_env(ref)
        elif ref.provider == "file":
            result = self._resolve_file(ref)
        elif ref.provider == "vault":
            result = self._resolve_vault(ref)
        elif ref.provider == "aws_sm":
            result = self._resolve_aws_sm(ref)
        else:
            raise ConfigError(f"Unknown secret provider: {ref.provider}")

        self._cache[cache_key] = result
        return result

    def _resolve_env(self, ref: SecretRef) -> str:
        if not ref.key:
            raise ConfigError("env provider requires 'key' field")
        value = os.environ.get(ref.key)
        if value is None:
            if ref.default is not None:
                return ref.default
            raise ConfigError(f"Environment variable not found: {ref.key}")
        return value

    def _resolve_file(self, ref: SecretRef) -> str:
        if not ref.path:
            raise ConfigError("file provider requires 'path' field")
        file_path = Path(ref.path)
        if not file_path.exists():
            raise ConfigError(f"Secret file not found: {ref.path}")
        return file_path.read_text().strip()

    def _resolve_vault(self, ref: SecretRef) -> str:
        raise ConfigError("Vault provider not yet implemented")

    def _resolve_aws_sm(self, ref: SecretRef) -> str:
        raise ConfigError("AWS Secrets Manager provider not yet implemented")
