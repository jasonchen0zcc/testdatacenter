import asyncio
import pytest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch

from tdc.config.watcher import ConfigWatcher, FileChange
from tdc.config.cache import ConfigCache
from tdc.config.loader import ConfigLoader
from tdc.config.models import TaskConfig, TargetDBConfig
from tdc.core.constants import TaskType
from tdc.core.exceptions import ConfigError


@pytest.fixture
def config_dir(tmp_path):
    """创建临时配置目录"""
    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    tasks_dir = config_dir / "tasks"
    tasks_dir.mkdir()
    return config_dir


@pytest.fixture
def config_cache():
    """创建 ConfigCache 实例"""
    return ConfigCache()


@pytest.fixture
def sample_config():
    """创建示例 TaskConfig"""
    return TaskConfig(
        task_id="test_task",
        task_name="Test Task",
        task_type=TaskType.DIRECT_INSERT,
        schedule="0 2 * * *",
        target_db=TargetDBConfig(
            instance="test_db",
            database="test_db"
        ),
        data_template={
            "table": "test_table",
            "batch_size": 100,
            "total_count": 1000,
            "fields": {}
        }
    )


@pytest.fixture
def mock_loader():
    """创建 Mock ConfigLoader"""
    loader = MagicMock(spec=ConfigLoader)
    return loader


@pytest.fixture
def watcher(config_dir, mock_loader, config_cache):
    """创建 ConfigWatcher 实例"""
    return ConfigWatcher(
        config_dir=config_dir,
        loader=mock_loader,
        cache=config_cache,
        check_interval=0.1
    )


class TestConfigWatcher:
    """ConfigWatcher 测试类"""

    @pytest.mark.asyncio
    async def test_start_stop(self, watcher):
        """测试启动和停止监听"""
        # 启动
        await watcher.start()
        assert watcher._running is True
        assert watcher._task is not None

        # 停止
        await watcher.stop()
        assert watcher._running is False

    @pytest.mark.asyncio
    async def test_start_already_running(self, watcher):
        """测试重复启动警告"""
        await watcher.start()

        with patch("tdc.config.watcher.logger") as mock_logger:
            await watcher.start()
            mock_logger.warning.assert_called_once()

        await watcher.stop()

    @pytest.mark.asyncio
    async def test_stop_not_running(self, watcher):
        """测试停止未运行的监听器"""
        # 应该不抛出异常
        await watcher.stop()
        assert watcher._running is False

    @pytest.mark.asyncio
    async def test_on_reload_callback(self, watcher):
        """测试设置重载回调"""
        callback_called = False
        callback_task_id = None
        callback_success = None

        def on_reload(task_id: str, success: bool):
            nonlocal callback_called, callback_task_id, callback_success
            callback_called = True
            callback_task_id = task_id
            callback_success = success

        watcher.on_reload(on_reload)
        assert watcher._on_reload is on_reload

    @pytest.mark.asyncio
    async def test_load_new_task(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试加载新任务"""
        # 创建任务文件
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\ntask_name: Test Task\n")

        mock_loader.load_task_file.return_value = sample_config

        # 手动调用 _load_new_task
        mtime = task_file.stat().st_mtime
        await watcher._load_new_task(task_file, mtime)

        # 验证缓存
        cached = await config_cache.get("test_task")
        assert cached is not None
        assert cached.task_id == "test_task"

    @pytest.mark.asyncio
    async def test_reload_task(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试重新加载任务"""
        # 先设置初始缓存
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\ntask_name: Old Task\n")
        initial_mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, initial_mtime, task_file)

        # 更新配置
        updated_config = sample_config.model_copy(update={"task_name": "Updated Task"})
        mock_loader.load_task_file.return_value = updated_config

        # 模拟文件修改时间变化
        new_mtime = initial_mtime + 1

        # 重新加载
        await watcher._reload_task("test_task", task_file, new_mtime)

        # 验证缓存已更新
        cached = await config_cache.get("test_task")
        assert cached.task_name == "Updated Task"

    @pytest.mark.asyncio
    async def test_reload_task_while_running(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试任务运行时跳过重载"""
        # 设置缓存
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\n")
        initial_mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, initial_mtime, task_file)

        # 模拟任务正在运行
        with patch.object(config_cache, "is_task_running", return_value=True):
            with patch("tdc.config.watcher.logger") as mock_logger:
                await watcher._reload_task("test_task", task_file, initial_mtime + 1)
                mock_logger.warning.assert_called_once()

    @pytest.mark.asyncio
    async def test_find_task_id_for_file(self, watcher, config_dir, sample_config, config_cache):
        """测试根据文件路径查找 task_id"""
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\n")

        # 设置缓存
        mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, mtime, task_file)

        # 查找
        found_task_id = await watcher._find_task_id_for_file(task_file)
        assert found_task_id == "test_task"

    @pytest.mark.asyncio
    async def test_find_task_id_for_file_not_found(self, watcher, config_dir):
        """测试查找不存在的文件"""
        other_file = config_dir / "tasks" / "other.yaml"
        other_file.write_text("task_id: other\n")

        found_task_id = await watcher._find_task_id_for_file(other_file)
        assert found_task_id is None

    @pytest.mark.asyncio
    async def test_find_file_for_task(self, watcher, config_dir, sample_config, config_cache):
        """测试根据 task_id 查找文件路径"""
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\n")

        mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, mtime, task_file)

        found_path = await watcher._find_file_for_task("test_task")
        assert found_path == task_file

    @pytest.mark.asyncio
    async def test_force_reload_single_task(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试强制重载单个任务"""
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\n")
        initial_mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, initial_mtime, task_file)

        updated_config = sample_config.model_copy(update={"task_name": "Force Updated"})
        mock_loader.load_task_file.return_value = updated_config

        await watcher.force_reload("test_task")

        cached = await config_cache.get("test_task")
        assert cached.task_name == "Force Updated"

    @pytest.mark.asyncio
    async def test_force_reload_all(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试强制重载所有任务"""
        # 创建两个任务文件
        task_file1 = config_dir / "tasks" / "task1.yaml"
        task_file1.write_text("task_id: task1\n")
        task_file2 = config_dir / "tasks" / "task2.yaml"
        task_file2.write_text("task_id: task2\n")

        mtime1 = task_file1.stat().st_mtime
        mtime2 = task_file2.stat().st_mtime
        await config_cache.set("task1", sample_config, mtime1, task_file1)

        config2 = sample_config.model_copy(update={"task_id": "task2"})
        await config_cache.set("task2", config2, mtime2, task_file2)

        updated_config1 = sample_config.model_copy(update={"task_id": "task1", "task_name": "Updated1"})
        updated_config2 = config2.model_copy(update={"task_id": "task2", "task_name": "Updated2"})
        mock_loader.load_task_configs.return_value = [updated_config1, updated_config2]

        await watcher.force_reload()

        # 验证两个任务都被更新
        cached1 = await config_cache.get("task1")
        cached2 = await config_cache.get("task2")
        assert cached1.task_name == "Updated1"
        assert cached2.task_name == "Updated2"

    @pytest.mark.asyncio
    async def test_check_changes_detects_new_file(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试检查变更检测到新文件"""
        # 创建任务文件
        task_file = config_dir / "tasks" / "new_task.yaml"
        task_file.write_text("task_id: new_task\n")

        mock_loader.load_task_file.return_value = sample_config.model_copy(update={"task_id": "new_task"})

        # 检查变更
        await watcher._check_changes()

        # 验证新任务被加载
        cached = await config_cache.get("new_task")
        assert cached is not None
        assert cached.task_id == "new_task"

    @pytest.mark.asyncio
    async def test_check_changes_detects_modified_file(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试检查变更检测到文件修改"""
        # 创建并缓存初始任务
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\n")
        initial_mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, initial_mtime, task_file)

        # 模拟更新的配置
        updated_config = sample_config.model_copy(update={"task_name": "Modified Task"})
        mock_loader.load_task_file.return_value = updated_config

        # 模拟文件修改（通过修改缓存的 mtime 使其小于新 mtime）
        await config_cache.set("test_task", sample_config, initial_mtime - 1, task_file)

        # 检查变更
        await watcher._check_changes()

        # 验证任务被重新加载
        cached = await config_cache.get("test_task")
        assert cached.task_name == "Modified Task"

    @pytest.mark.asyncio
    async def test_load_new_task_config_error(self, watcher, config_dir, mock_loader):
        """测试加载新任务时配置错误处理"""
        task_file = config_dir / "tasks" / "invalid.yaml"
        task_file.write_text("invalid: yaml: content")

        mock_loader.load_task_file.side_effect = ConfigError("Invalid config")

        with patch("tdc.config.watcher.logger") as mock_logger:
            await watcher._load_new_task(task_file, 1234567890.0)
            mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_reload_task_config_error(self, watcher, config_dir, mock_loader, sample_config, config_cache):
        """测试重载任务时配置错误处理"""
        task_file = config_dir / "tasks" / "test_task.yaml"
        task_file.write_text("task_id: test_task\n")
        initial_mtime = task_file.stat().st_mtime
        await config_cache.set("test_task", sample_config, initial_mtime, task_file)

        mock_loader.load_task_file.side_effect = ConfigError("Invalid config")

        callback_called = False
        def on_reload(task_id: str, success: bool):
            nonlocal callback_called
            if not success:
                callback_called = True

        watcher.on_reload(on_reload)

        with patch("tdc.config.watcher.logger") as mock_logger:
            await watcher._reload_task("test_task", task_file, initial_mtime + 1)
            mock_logger.error.assert_called_once()

        assert callback_called is True

    @pytest.mark.asyncio
    async def test_reload_all_config_error(self, watcher, mock_loader):
        """测试重载所有配置时错误处理"""
        mock_loader.load_task_configs.side_effect = ConfigError("Failed to load configs")

        callback_called = False
        def on_reload(task_id: str, success: bool):
            nonlocal callback_called
            if task_id == "*" and not success:
                callback_called = True

        watcher.on_reload(on_reload)

        with patch("tdc.config.watcher.logger") as mock_logger:
            await watcher._reload_all()
            mock_logger.error.assert_called_once()

        assert callback_called is True

    @pytest.mark.asyncio
    async def test_watch_loop_error_handling(self, watcher):
        """测试监听循环错误处理"""
        watcher._running = True

        with patch.object(watcher, "_check_changes", side_effect=Exception("Test error")):
            with patch("tdc.config.watcher.logger") as mock_logger:
                # 运行一次循环后停止
                async def stop_after_one():
                    await asyncio.sleep(0.05)
                    watcher._running = False

                await asyncio.gather(
                    watcher._watch_loop(),
                    stop_after_one()
                )
                mock_logger.error.assert_called_once()

    @pytest.mark.asyncio
    async def test_check_changes_os_error(self, watcher, config_dir):
        """测试检查变更时文件访问错误处理"""
        # 创建一个会被 OSError 的文件路径
        with patch.object(Path, "stat", side_effect=OSError("Permission denied")):
            # 应该不抛出异常
            await watcher._check_changes()
