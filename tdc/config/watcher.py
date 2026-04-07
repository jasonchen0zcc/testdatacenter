import asyncio
import logging
from pathlib import Path
from typing import Callable, Optional
from dataclasses import dataclass

from tdc.config.cache import ConfigCache
from tdc.config.loader import ConfigLoader
from tdc.core.exceptions import ConfigError

logger = logging.getLogger(__name__)


@dataclass
class FileChange:
    """文件变更信息"""
    path: Path
    old_mtime: Optional[float]
    new_mtime: float


class ConfigWatcher:
    """配置热加载监听器"""

    def __init__(
        self,
        config_dir: Path,
        loader: ConfigLoader,
        cache: ConfigCache,
        check_interval: float = 5.0
    ):
        self.config_dir = config_dir
        self.loader = loader
        self.cache = cache
        self.check_interval = check_interval
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._on_reload: Optional[Callable[[str, bool], None]] = None

    def on_reload(self, callback: Callable[[str, bool], None]):
        """设置重载回调函数 (task_id, success)"""
        self._on_reload = callback

    async def start(self):
        """启动监听"""
        if self._running:
            logger.warning("ConfigWatcher already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._watch_loop())
        logger.info(f"ConfigWatcher started, watching {self.config_dir}")

    async def stop(self):
        """停止监听"""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("ConfigWatcher stopped")

    async def _watch_loop(self):
        """监听循环"""
        while self._running:
            try:
                await self._check_changes()
            except Exception as e:
                logger.error(f"Error checking config changes: {e}")

            try:
                await asyncio.sleep(self.check_interval)
            except asyncio.CancelledError:
                break

    async def _check_changes(self):
        """检查文件变更"""
        yaml_files = list(self.config_dir.rglob("*.yaml"))

        for file_path in yaml_files:
            try:
                stat = file_path.stat()
                current_mtime = stat.st_mtime
            except OSError:
                continue

            task_id = await self._find_task_id_for_file(file_path)
            if not task_id:
                # 检查是否是 base/common 配置变更
                if file_path.parent.name in ("base", "common"):
                    await self._reload_all()
                    return
                # 尝试作为新任务加载
                await self._load_new_task(file_path, current_mtime)
                continue

            cached_mtime = await self.cache.get_mtime(task_id)

            if cached_mtime is None:
                await self._load_new_task(file_path, current_mtime)
            elif current_mtime > cached_mtime:
                await self._reload_task(task_id, file_path, current_mtime)

    async def _find_task_id_for_file(self, file_path: Path) -> Optional[str]:
        """根据文件路径查找 task_id"""
        all_cached = await self.cache.list_all()
        for task_id, cached in all_cached.items():
            if cached.file_path == file_path:
                return task_id
        return None

    async def _load_new_task(self, file_path: Path, mtime: float):
        """加载新任务"""
        try:
            config = self.loader.load_task_file(file_path)
            await self.cache.set(config.task_id, config, mtime, file_path)
            logger.info(f"New task loaded: {config.task_id}")
            if self._on_reload:
                self._on_reload(config.task_id, True)
        except ConfigError as e:
            logger.error(f"Failed to load new task from {file_path}: {e}")

    async def _reload_task(self, task_id: str, file_path: Path, mtime: float):
        """重新加载任务"""
        if self.cache.is_task_running(task_id):
            logger.warning(f"Task {task_id} is running, skipping reload")
            return

        try:
            config = self.loader.load_task_file(file_path)
            await self.cache.set(task_id, config, mtime, file_path)
            logger.info(f"Task reloaded: {task_id}")
            if self._on_reload:
                self._on_reload(task_id, True)
        except ConfigError as e:
            logger.error(f"Failed to reload task {task_id}: {e}")
            if self._on_reload:
                self._on_reload(task_id, False)

    async def _reload_all(self):
        """重新加载所有配置"""
        logger.info("Base config changed, reloading all tasks")
        try:
            configs = self.loader.load_task_configs()
            for config in configs:
                file_path = await self._find_file_for_task(config.task_id)
                if file_path and file_path.exists():
                    mtime = file_path.stat().st_mtime
                else:
                    # 新任务，尝试在 tasks 目录中查找
                    tasks_dir = self.config_dir / "tasks"
                    file_path = tasks_dir / f"{config.task_id}.yaml"
                    if not file_path.exists():
                        # 尝试递归查找
                        for yaml_file in tasks_dir.rglob("*.yaml"):
                            try:
                                content = yaml_file.read_text()
                                if f"task_id: {config.task_id}" in content:
                                    file_path = yaml_file
                                    break
                            except OSError:
                                continue
                    if not file_path.exists():
                        continue
                    mtime = file_path.stat().st_mtime
                await self.cache.set(config.task_id, config, mtime, file_path)
            if self._on_reload:
                self._on_reload("*", True)
        except ConfigError as e:
            logger.error(f"Failed to reload all configs: {e}")
            if self._on_reload:
                self._on_reload("*", False)

    async def _find_file_for_task(self, task_id: str) -> Optional[Path]:
        """根据 task_id 查找文件路径"""
        return await self.cache.get_file_path(task_id)

    async def force_reload(self, task_id: Optional[str] = None):
        """强制重新加载"""
        if task_id:
            file_path = await self._find_file_for_task(task_id)
            if file_path and file_path.exists():
                mtime = file_path.stat().st_mtime
                await self._reload_task(task_id, file_path, mtime)
        else:
            await self._reload_all()
