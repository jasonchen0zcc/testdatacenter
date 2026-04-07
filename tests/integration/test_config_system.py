"""Integration tests for config system optimization."""
import pytest
import asyncio
from pathlib import Path
from tdc.config.loader import ConfigLoader
from tdc.config.watcher import ConfigWatcher
from tdc.config.cache import ConfigCache
from tdc.core.exceptions import ConfigError


class TestConfigSystemIntegration:
    @pytest.fixture
    def setup_config_dir(self, tmp_path):
        """Create complete test config directory."""
        # base/default.yaml
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        (base_dir / "default.yaml").write_text("""
base_id: "default"
target_db:
  instance: "default_db"
  database: "test"
execution:
  iterations: 10
  user_source: "faker"
""")

        # tasks/test.yaml
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "test.yaml").write_text("""
task_id: "integration_test"
task_name: "Integration Test"
task_type: "http_source"
schedule: "0 2 * * *"
extends: "base/default"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        return tmp_path

    def test_full_load_flow(self, setup_config_dir, monkeypatch):
        """Test complete loading flow."""
        monkeypatch.setenv("DB_PASS", "secret123")

        loader = ConfigLoader(str(setup_config_dir))
        configs = loader.load_task_configs()

        assert len(configs) == 1
        config = configs[0]
        assert config.task_id == "integration_test"
        assert config.execution.iterations == 10  # Inherited value
        assert config.target_db.instance == "default_db"

    @pytest.mark.asyncio
    async def test_hot_reload_flow(self, setup_config_dir):
        """Test hot reload flow."""
        loader = ConfigLoader(str(setup_config_dir))
        cache = ConfigCache()
        watcher = ConfigWatcher(setup_config_dir, loader, cache, check_interval=0.1)

        reload_events = []

        def on_reload(task_id, success):
            reload_events.append((task_id, success))

        watcher.on_reload(on_reload)

        # Start watching
        await watcher.start()

        # Wait for initial check
        await asyncio.sleep(0.15)

        # Modify config file
        task_file = setup_config_dir / "tasks" / "test.yaml"
        content = task_file.read_text()
        task_file.write_text(content.replace("iterations: 10", "iterations: 20"))

        # Wait for change detection
        await asyncio.sleep(0.15)

        # Stop watching
        await watcher.stop()

        # Verify reload event
        assert len(reload_events) > 0

    def test_inheritance_with_secret_resolution(self, tmp_path, monkeypatch):
        """Test inheritance combined with secret resolution."""
        monkeypatch.setenv("API_KEY", "secret_key")

        # Create base with partial config
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        (base_dir / "api.yaml").write_text("""
base_id: "api"
target_db:
  instance: "api_db"
  database: "api"
""")

        # Create task inheriting base with secret
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "api_task.yaml").write_text("""
task_id: "api_task"
task_name: "API Task"
task_type: "http_source"
schedule: "0 2 * * *"
extends: "base/api"
pipeline:
  - step_id: "call_api"
    http:
      url: "http://api.example.com"
      method: GET
      headers:
        X-API-Key:
          provider: "env"
          key: "API_KEY"
""")

        loader = ConfigLoader(str(tmp_path))
        config = loader.load_task_by_id("api_task")

        # Verify inheritance worked
        assert config.target_db.instance == "api_db"
        # Verify secret resolution worked
        assert config.pipeline[0].http.headers["X-API-Key"] == "secret_key"

    def test_multiple_inheritance(self, tmp_path):
        """Test multiple inheritance."""
        # Create two base configs
        base_dir = tmp_path / "base"
        base_dir.mkdir()

        (base_dir / "db.yaml").write_text("""
base_id: "db"
target_db:
  instance: "shared_db"
  database: "test"
""")

        (base_dir / "exec.yaml").write_text("""
base_id: "exec"
execution:
  iterations: 100
  delay_ms: 50
""")

        # Create task with multiple inheritance
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "multi.yaml").write_text("""
task_id: "multi_inherit"
task_name: "Multi Inherit"
task_type: "http_source"
schedule: "0 2 * * *"
extends:
  - "base/db"
  - "base/exec"
execution:
  iterations: 50
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        config = loader.load_task_by_id("multi_inherit")

        # Verify both bases were merged
        assert config.target_db.instance == "shared_db"
        assert config.execution.iterations == 50  # Override
        assert config.execution.delay_ms == 50  # Inherited

    def test_circular_inheritance_detection(self, tmp_path):
        """Test circular inheritance is detected."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()

        (base_dir / "a.yaml").write_text("""
base_id: "a"
extends: "base/b"
execution:
  iterations: 1
""")

        (base_dir / "b.yaml").write_text("""
base_id: "b"
extends: "base/a"
execution:
  iterations: 2
""")

        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "circular.yaml").write_text("""
task_id: "circular"
task_name: "Circular"
task_type: "http_source"
schedule: "0 2 * * *"
extends: "base/a"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        with pytest.raises(ConfigError, match="Circular inheritance"):
            loader.load_task_by_id("circular")

    def test_nested_directories(self, tmp_path):
        """Test loading from nested directories."""
        tasks_dir = tmp_path / "tasks"

        # Create nested structure
        order_dir = tasks_dir / "order"
        order_dir.mkdir(parents=True)

        user_dir = tasks_dir / "user"
        user_dir.mkdir(parents=True)

        (order_dir / "create.yaml").write_text("""
task_id: "order_create"
task_name: "Create Order"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "order_db"
  database: "orders"
pipeline:
  - step_id: "create"
    http:
      url: "http://api/orders"
      method: POST
""")

        (user_dir / "register.yaml").write_text("""
task_id: "user_register"
task_name: "Register User"
task_type: "http_source"
schedule: "0 3 * * *"
target_db:
  instance: "user_db"
  database: "users"
pipeline:
  - step_id: "register"
    http:
      url: "http://api/users"
      method: POST
""")

        loader = ConfigLoader(str(tmp_path))
        configs = loader.load_task_configs()

        assert len(configs) == 2
        task_ids = {c.task_id for c in configs}
        assert task_ids == {"order_create", "user_register"}
