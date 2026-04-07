import asyncio
from typing import Dict, Optional
from pathlib import Path
from dataclasses import dataclass

from tdc.config.models import TaskConfig


@dataclass
class CachedConfig:
    """缓存的配置项"""
    config: TaskConfig
    mtime: float
    file_path: Path


class ConfigCache:
    """配置缓存管理器"""

    def __init__(self):
        self._configs: Dict[str, CachedConfig] = {}
        self._lock = asyncio.Lock()

    async def get(self, task_id: str) -> Optional[TaskConfig]:
        """获取缓存的配置"""
        async with self._lock:
            cached = self._configs.get(task_id)
            return cached.config if cached else None

    async def set(self, task_id: str, config: TaskConfig, mtime: float, file_path: Path):
        """设置缓存"""
        async with self._lock:
            self._configs[task_id] = CachedConfig(
                config=config,
                mtime=mtime,
                file_path=file_path
            )

    async def remove(self, task_id: str):
        """移除缓存"""
        async with self._lock:
            self._configs.pop(task_id, None)

    async def get_mtime(self, task_id: str) -> Optional[float]:
        """获取缓存的文件修改时间"""
        async with self._lock:
            cached = self._configs.get(task_id)
            return cached.mtime if cached else None

    async def get_file_path(self, task_id: str) -> Optional[Path]:
        """获取缓存的文件路径"""
        async with self._lock:
            cached = self._configs.get(task_id)
            return cached.file_path if cached else None

    async def clear(self):
        """清空缓存"""
        async with self._lock:
            self._configs.clear()

    async def list_all(self) -> Dict[str, CachedConfig]:
        """获取所有缓存（副本）"""
        async with self._lock:
            return dict(self._configs)

    def is_task_running(self, task_id: str) -> bool:
        """检查任务是否正在运行（预留接口）"""
        return False
