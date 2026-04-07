"""Tests for ConfigLoader with inheritance and secret resolution."""
import pytest
from pathlib import Path
from tdc.config.loader import ConfigLoader
from tdc.core.exceptions import ConfigError


class TestConfigLoaderInheritance:
    def test_load_task_with_inheritance(self, tmp_path):
        """Test loading task with inheritance."""
        # Create base config
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        (base_dir / "default.yaml").write_text("""
base_id: "default"
target_db:
  instance: "default_db"
  database: "test"
execution:
  iterations: 100
  user_source: "faker"
""")

        # Create task config
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "test.yaml").write_text("""
task_id: "test_inherit"
task_name: "Test Inheritance"
task_type: "http_source"
schedule: "0 2 * * *"
extends: "base/default"
execution:
  iterations: 50
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        config = loader.load_task_by_id("test_inherit")

        assert config.execution.iterations == 50  # Child overrides
        assert config.target_db.instance == "default_db"  # Inherited

    def test_load_task_with_secret_ref(self, tmp_path, monkeypatch):
        """Test loading task with secret reference in http header."""
        monkeypatch.setenv("API_KEY", "secret123")

        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "test.yaml").write_text("""
task_id: "test_secret"
task_name: "Test Secret"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
      headers:
        X-API-Key:
          provider: "env"
          key: "API_KEY"
""")

        loader = ConfigLoader(str(tmp_path))
        config = loader.load_task_by_id("test_secret")

        assert config.pipeline[0].http.headers["X-API-Key"] == "secret123"

    def test_load_task_with_env_var_in_string(self, tmp_path, monkeypatch):
        """Test loading task with environment variable in string."""
        monkeypatch.setenv("API_URL", "https://api.example.com")

        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "test.yaml").write_text("""
task_id: "test_env"
task_name: "Test Env"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "call_api"
    http:
      url: "${API_URL}/endpoint"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        config = loader.load_task_by_id("test_env")

        assert config.pipeline[0].http.url == "https://api.example.com/endpoint"

    def test_load_task_file_directly(self, tmp_path):
        """Test loading task file directly."""
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        task_file = tasks_dir / "direct.yaml"
        task_file.write_text("""
task_id: "direct_load"
task_name: "Direct Load"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        config = loader.load_task_file(task_file)

        assert config.task_id == "direct_load"
        assert config.task_name == "Direct Load"

    def test_skip_underscore_files(self, tmp_path):
        """Test that files starting with _ are skipped."""
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()
        (tasks_dir / "_index.yaml").write_text("auto_discover: true")
        (tasks_dir / "valid.yaml").write_text("""
task_id: "valid_task"
task_name: "Valid Task"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        configs = loader.load_task_configs()

        assert len(configs) == 1
        assert configs[0].task_id == "valid_task"

    def test_duplicate_task_id_detection(self, tmp_path):
        """Test detection of duplicate task IDs."""
        tasks_dir = tmp_path / "tasks"
        tasks_dir.mkdir()

        # Create two tasks with same ID
        (tasks_dir / "task1.yaml").write_text("""
task_id: "duplicate"
task_name: "Task 1"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        sub_dir = tasks_dir / "subdir"
        sub_dir.mkdir()
        (sub_dir / "task2.yaml").write_text("""
task_id: "duplicate"
task_name: "Task 2"
task_type: "http_source"
schedule: "0 3 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        with pytest.raises(ConfigError, match="Duplicate task_id"):
            loader.load_task_configs()

    def test_load_base_config(self, tmp_path):
        """Test loading base config directly."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        (base_dir / "mybase.yaml").write_text("""
base_id: "mybase"
execution:
  iterations: 50
""")

        loader = ConfigLoader(str(tmp_path))
        base = loader.load_base_config("base/mybase")

        assert base["execution"]["iterations"] == 50

    def test_nested_task_directories(self, tmp_path):
        """Test loading tasks from nested directories."""
        tasks_dir = tmp_path / "tasks"
        order_dir = tasks_dir / "order"
        order_dir.mkdir(parents=True)

        (order_dir / "create.yaml").write_text("""
task_id: "order_create"
task_name: "Create Order"
task_type: "http_source"
schedule: "0 2 * * *"
target_db:
  instance: "test"
  database: "test"
pipeline:
  - step_id: "test"
    http:
      url: "http://test.com"
      method: GET
""")

        loader = ConfigLoader(str(tmp_path))
        configs = loader.load_task_configs()

        assert len(configs) == 1
        assert configs[0].task_id == "order_create"
