from typing import Dict, List, Any, Union
from pathlib import Path
import yaml

from tdc.core.exceptions import ConfigError


class InheritanceResolver:
    """配置继承解析器"""

    def __init__(self, config_dir: Path):
        self.config_dir = config_dir
        self._base_cache: Dict[str, Dict] = {}
        self._inheritance_chain: List[str] = []

    def resolve(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """解析配置继承"""
        extends = config.get("extends")
        if not extends:
            return config

        # 检测循环依赖
        task_id = config.get("task_id") or config.get("base_id")
        if task_id in self._inheritance_chain:
            raise ConfigError(f"Circular inheritance detected: {' -> '.join(self._inheritance_chain)} -> {task_id}")

        self._inheritance_chain.append(task_id or "unknown")

        try:
            # 加载基础配置
            base_configs = []
            if isinstance(extends, str):
                base_configs.append(self._load_base(extends))
            else:
                for base_ref in extends:
                    base_configs.append(self._load_base(base_ref))

            # 深度合并：先合并所有基础配置，再合并当前配置
            merged = {}
            for base in base_configs:
                merged = self._deep_merge(merged, base)

            merged = self._deep_merge(merged, config)

            # 移除继承元数据字段
            merged.pop("extends", None)
            merged.pop("base_id", None)

            return merged
        finally:
            self._inheritance_chain.pop()

    def _load_base(self, ref: str) -> Dict[str, Any]:
        """加载基础配置"""
        if ref in self._base_cache:
            return self._base_cache[ref]

        # 解析引用路径: "base/order_db" -> configs/base/order_db.yaml
        parts = ref.split("/")
        if len(parts) != 2:
            raise ConfigError(f"Invalid base config reference: {ref}, expected format: 'dir/name'")

        dir_name, file_name = parts
        base_file = self.config_dir / dir_name / f"{file_name}.yaml"

        if not base_file.exists():
            raise ConfigError(f"Base config file not found: {base_file}")

        with open(base_file) as f:
            base_config = yaml.safe_load(f)

        # 递归解析基础配置的继承
        resolved = self.resolve(base_config)
        self._base_cache[ref] = resolved
        return resolved

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """深度合并两个字典"""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            elif key in result and isinstance(result[key], list) and isinstance(value, list):
                # 数组完全替换
                result[key] = value
            else:
                result[key] = value
        return result
