import os
import re
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any

import yaml

from tdc.config.models import TaskConfig, DBConfig, SecretRef
from tdc.config.inheritance import InheritanceResolver
from tdc.config.secrets import SecretResolver
from tdc.config.cache import ConfigCache
from tdc.core.exceptions import ConfigError


def load_dotenv(project_root: Path = None) -> None:
    """Load environment variables from .env file if exists"""
    if project_root is None:
        current = Path.cwd()
        for parent in [current] + list(current.parents):
            if (parent / ".git").exists() or (parent / "pyproject.toml").exists():
                project_root = parent
                break

    if project_root is None:
        return

    dotenv_file = project_root / ".env"
    if not dotenv_file.exists():
        return

    with open(dotenv_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                key = key.strip()
                value = value.strip().strip('"\'')
                if key not in os.environ:
                    os.environ[key] = value


def expand_env_vars(content: str) -> str:
    """扩展环境变量，支持 ${VAR} 和 ${VAR:-default} 格式"""
    pattern = re.compile(r'\$\{([^}]+)\}')

    def replace(match):
        var_expr = match.group(1)
        if ':-' in var_expr:
            var_name, default = var_expr.split(':-', 1)
            return os.environ.get(var_name, default)
        else:
            return os.environ.get(var_expr, match.group(0))

    return pattern.sub(replace, content)


class ConfigLoader:
    _dotenv_loaded = False

    def __init__(
        self,
        config_dir: str,
        enable_cache: bool = True,
        enable_hot_reload: bool = False
    ):
        self.config_dir = Path(config_dir)
        self.inheritance_resolver = InheritanceResolver(self.config_dir)
        self.secret_resolver = SecretResolver()
        self.cache = ConfigCache() if enable_cache else None
        self._hot_reload_enabled = enable_hot_reload

        if not ConfigLoader._dotenv_loaded:
            load_dotenv()
            ConfigLoader._dotenv_loaded = True

    def load_db_config(self) -> DBConfig:
        """加载数据库配置"""
        db_file = self.config_dir / "db.yaml"
        if not db_file.exists():
            raise ConfigError(f"DB config file not found: {db_file}")

        content = db_file.read_text()
        content = expand_env_vars(content)
        data = yaml.safe_load(content)

        # 解析密钥引用
        data = self.secret_resolver.resolve_config(data)

        return DBConfig(**data)

    def load_task_configs(self) -> List[TaskConfig]:
        """加载所有任务配置"""
        tasks_dir = self.config_dir / "tasks"
        if not tasks_dir.exists():
            raise ConfigError(f"Tasks directory not found: {tasks_dir}")

        configs = []
        seen_ids = set()

        # 递归查找所有子目录中的 yaml 文件
        for task_file in tasks_dir.rglob("*.yaml"):
            # 跳过索引文件
            if task_file.name.startswith("_"):
                continue

            try:
                config = self.load_task_file(task_file)
                if config.task_id in seen_ids:
                    raise ConfigError(f"Duplicate task_id: {config.task_id}")
                seen_ids.add(config.task_id)
                configs.append(config)
            except ConfigError as e:
                raise ConfigError(f"Failed to load {task_file}: {e}")

        return configs

    def load_task_file(self, file_path: Path) -> TaskConfig:
        """从文件加载单个任务配置"""
        if not file_path.exists():
            raise ConfigError(f"Task file not found: {file_path}")

        content = file_path.read_text()
        content = expand_env_vars(content)
        data = yaml.safe_load(content)

        # 解析继承
        data = self.inheritance_resolver.resolve(data)

        # 解析密钥
        data = self.secret_resolver.resolve_config(data)

        return TaskConfig(**data)

    async def load_task_by_id(self, task_id: str) -> TaskConfig:
        """根据 ID 加载任务配置"""
        # 先检查缓存
        if self.cache:
            cached = await self.cache.get(task_id)
            if cached:
                return cached

        # 从文件加载
        tasks_dir = self.config_dir / "tasks"
        for task_file in tasks_dir.rglob("*.yaml"):
            if task_file.name.startswith("_"):
                continue
            try:
                config = self.load_task_file(task_file)
                if config.task_id == task_id:
                    # 存入缓存
                    if self.cache:
                        mtime = task_file.stat().st_mtime
                        await self.cache.set(task_id, config, mtime, task_file)
                    return config
            except ConfigError as e:
                if "Circular inheritance" in str(e):
                    raise
                continue

        raise ConfigError(f"Task config not found: {task_id}")

    def load_base_config(self, base_id: str) -> Dict[str, Any]:
        """加载基础配置"""
        return self.inheritance_resolver._load_base(base_id)
