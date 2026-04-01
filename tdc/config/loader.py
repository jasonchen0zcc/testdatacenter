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
        seen_ids = set()
        for task_file in tasks_dir.rglob("*.yaml"):
            content = task_file.read_text()
            content = os.path.expandvars(content)
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
