import asyncio
import pytest
from pathlib import Path
from unittest.mock import MagicMock

from tdc.config.cache import ConfigCache, CachedConfig
from tdc.config.models import TaskConfig, TargetDBConfig
from tdc.core.constants import TaskType


@pytest.fixture
def config_cache():
    """创建空的 ConfigCache 实例"""
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
def sample_path():
    """创建示例 Path"""
    return Path("/tmp/test_config.yaml")


class TestConfigCache:
    """ConfigCache 测试类"""

    @pytest.mark.asyncio
    async def test_get_empty_cache(self, config_cache):
        """测试从空缓存获取返回 None"""
        result = await config_cache.get("non_existent_task")
        assert result is None

    @pytest.mark.asyncio
    async def test_set_and_get(self, config_cache, sample_config, sample_path):
        """测试设置和获取缓存"""
        mtime = 1234567890.0

        await config_cache.set("test_task", sample_config, mtime, sample_path)
        result = await config_cache.get("test_task")

        assert result is not None
        assert result.task_id == "test_task"
        assert result.task_name == "Test Task"

    @pytest.mark.asyncio
    async def test_get_mtime(self, config_cache, sample_config, sample_path):
        """测试获取缓存的文件修改时间"""
        mtime = 1234567890.0

        await config_cache.set("test_task", sample_config, mtime, sample_path)
        result = await config_cache.get_mtime("test_task")

        assert result == mtime

    @pytest.mark.asyncio
    async def test_get_mtime_not_exists(self, config_cache):
        """测试获取不存在任务的 mtime 返回 None"""
        result = await config_cache.get_mtime("non_existent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_file_path(self, config_cache, sample_config, sample_path):
        """测试获取缓存的文件路径"""
        mtime = 1234567890.0

        await config_cache.set("test_task", sample_config, mtime, sample_path)
        result = await config_cache.get_file_path("test_task")

        assert result == sample_path

    @pytest.mark.asyncio
    async def test_get_file_path_not_exists(self, config_cache):
        """测试获取不存在任务的文件路径返回 None"""
        result = await config_cache.get_file_path("non_existent")
        assert result is None

    @pytest.mark.asyncio
    async def test_remove(self, config_cache, sample_config, sample_path):
        """测试移除缓存"""
        mtime = 1234567890.0

        await config_cache.set("test_task", sample_config, mtime, sample_path)
        await config_cache.remove("test_task")

        result = await config_cache.get("test_task")
        assert result is None

    @pytest.mark.asyncio
    async def test_remove_not_exists(self, config_cache):
        """测试移除不存在的任务不报错"""
        # 应该不抛出异常
        await config_cache.remove("non_existent")

    @pytest.mark.asyncio
    async def test_clear(self, config_cache, sample_config, sample_path):
        """测试清空缓存"""
        mtime = 1234567890.0

        await config_cache.set("task1", sample_config, mtime, sample_path)
        await config_cache.set("task2", sample_config, mtime, sample_path)

        await config_cache.clear()

        assert await config_cache.get("task1") is None
        assert await config_cache.get("task2") is None

    @pytest.mark.asyncio
    async def test_list_all(self, config_cache, sample_config, sample_path):
        """测试获取所有缓存"""
        mtime1 = 1234567890.0
        mtime2 = 1234567891.0

        await config_cache.set("task1", sample_config, mtime1, sample_path)
        await config_cache.set("task2", sample_config, mtime2, sample_path / "other.yaml")

        all_configs = await config_cache.list_all()

        assert len(all_configs) == 2
        assert "task1" in all_configs
        assert "task2" in all_configs
        assert all_configs["task1"].mtime == mtime1
        assert all_configs["task2"].mtime == mtime2

    @pytest.mark.asyncio
    async def test_list_all_returns_copy(self, config_cache, sample_config, sample_path):
        """测试 list_all 返回的是副本，修改不影响原缓存"""
        mtime = 1234567890.0

        await config_cache.set("task1", sample_config, mtime, sample_path)

        all_configs = await config_cache.list_all()
        all_configs.pop("task1")  # 修改返回的字典

        # 原缓存应该不受影响
        result = await config_cache.get("task1")
        assert result is not None

    def test_is_task_running(self, config_cache):
        """测试 is_task_running 预留接口"""
        # 默认实现返回 False
        assert config_cache.is_task_running("any_task") is False

    @pytest.mark.asyncio
    async def test_concurrent_access(self, config_cache, sample_config, sample_path):
        """测试并发访问安全性"""
        mtime = 1234567890.0
        iterations = 100

        async def writer(task_id_prefix: str):
            for i in range(iterations):
                await config_cache.set(
                    f"{task_id_prefix}_{i}",
                    sample_config,
                    mtime + i,
                    sample_path
                )

        async def reader(task_id_prefix: str):
            for i in range(iterations):
                await config_cache.get(f"{task_id_prefix}_{i}")

        # 并发执行多个写入和读取任务
        await asyncio.gather(
            writer("writer1"),
            writer("writer2"),
            reader("writer1"),
            reader("writer2"),
        )

        # 验证所有数据都正确写入
        all_configs = await config_cache.list_all()
        assert len(all_configs) == iterations * 2  # 两个 writer

    @pytest.mark.asyncio
    async def test_update_existing_task(self, config_cache, sample_config, sample_path):
        """测试更新已存在的任务缓存"""
        mtime1 = 1234567890.0
        mtime2 = 1234567891.0

        # 首次设置
        await config_cache.set("test_task", sample_config, mtime1, sample_path)

        # 更新配置
        updated_config = sample_config.model_copy(update={"task_name": "Updated Task"})
        await config_cache.set("test_task", updated_config, mtime2, sample_path / "updated.yaml")

        # 验证更新
        result = await config_cache.get("test_task")
        mtime_result = await config_cache.get_mtime("test_task")
        path_result = await config_cache.get_file_path("test_task")

        assert result.task_name == "Updated Task"
        assert mtime_result == mtime2
        assert path_result == sample_path / "updated.yaml"
