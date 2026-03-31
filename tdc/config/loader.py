import os
from pathlib import Path
from typing import Dict, List

import yaml

from tdc.config.models import TaskConfig, DBConfig
from tdc.core.exceptions import ConfigError


class ConfigLoader:
    def __init__(self, config_dir: str):
        self.config_dir = Path(config_dir)

    def load_db_config(self) -> DBConfig:
        db_file = self.config_dir / "db.yaml"
        if not db_file.exists():
            raise ConfigError(f"DB config file not found: {db_file}")

        content = db_file.read_text()
        # 环境变量替换
        content = os.path.expandvars(content)
        data = yaml.safe_load(content)
        return DBConfig(**data)

    def load_task_configs(self) -> List[TaskConfig]:
        tasks_dir = self.config_dir / "tasks"
        if not tasks_dir.exists():
            raise ConfigError(f"Tasks directory not found: {tasks_dir}")

        configs = []
        for task_file in tasks_dir.glob("*.yaml"):
            content = task_file.read_text()
            content = os.path.expandvars(content)
            data = yaml.safe_load(content)
            configs.append(TaskConfig(**data))

        return configs

    def load_task_by_id(self, task_id: str) -> TaskConfig:
        task_file = self.config_dir / "tasks" / f"{task_id}.yaml"
        if not task_file.exists():
            raise ConfigError(f"Task config not found: {task_file}")

        content = task_file.read_text()
        content = os.path.expandvars(content)
        data = yaml.safe_load(content)
        return TaskConfig(**data)
