import os
import re
from pathlib import Path
from typing import Dict, List

import yaml

from tdc.config.models import TaskConfig, DBConfig
from tdc.core.exceptions import ConfigError


def expand_env_vars(content: str) -> str:
    """扩展环境变量，支持 ${VAR} 和 ${VAR:-default} 格式"""
    # 匹配 ${VAR:-default} 或 ${VAR}
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
    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)

    def load_db_config(self) -> DBConfig:
        db_file = self.config_dir / "db.yaml"
        if not db_file.exists():
            raise ConfigError(f"DB config file not found: {db_file}")

        content = db_file.read_text()
        # 环境变量替换（支持 ${VAR} 和 ${VAR:-default} 格式）
        content = expand_env_vars(content)
        data = yaml.safe_load(content)
        return DBConfig(**data)

    def load_task_configs(self) -> List[TaskConfig]:
        tasks_dir = self.config_dir / "tasks"
        if not tasks_dir.exists():
            raise ConfigError(f"Tasks directory not found: {tasks_dir}")

        configs = []
        seen_ids = set()
        for task_file in tasks_dir.rglob("*.yaml"):
            content = task_file.read_text()
            content = expand_env_vars(content)
            data = yaml.safe_load(content)
            task = TaskConfig(**data)
            if task.task_id in seen_ids:
                raise ConfigError(f"Duplicate task_id: {task.task_id}")
            seen_ids.add(task.task_id)
            configs.append(task)

        return configs

    def load_task_by_id(self, task_id: str) -> TaskConfig:
        for task in self.load_task_configs():
            if task.task_id == task_id:
                return task
        raise ConfigError(f"Task config not found: {task_id}")
