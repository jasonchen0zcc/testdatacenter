import pytest
from tdc.config.models import (
    TaskConfig, HTTPConfig, PipelineStepConfig, FieldGeneratorConfig
)
from tdc.core.constants import TaskType


class TestTaskConfig:
    def test_http_source_config_validation(self):
        data = {
            "task_id": "test_http",
            "task_name": "Test HTTP Task",
            "task_type": "http_source",
            "schedule": "0 * * * *",
            "pipeline": [
                {
                    "step_id": "step1",
                    "http": {
                        "url": "https://api.example.com/test",
                        "method": "GET"
                    }
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST"
            },
            "target_db": {
                "instance": "test_db",
                "database": "test"
            }
        }
        config = TaskConfig(**data)
        assert config.task_id == "test_http"
        assert config.task_type.value == "http_source"

    def test_direct_insert_config_validation(self):
        data = {
            "task_id": "test_insert",
            "task_name": "Test Insert Task",
            "task_type": "direct_insert",
            "schedule": "0 * * * *",
            "data_template": {
                "table": "users",
                "batch_size": 100,
                "total_count": 1000,
                "fields": {
                    "id": {"type": "faker", "generator": "uuid4"}
                }
            },
            "tag_mapping": {
                "user_id": "{{ faker.uuid4 }}",
                "order_id": "N/A",
                "data_tag": "TEST"
            },
            "target_db": {
                "instance": "test_db",
                "database": "test"
            }
        }
        config = TaskConfig(**data)
        assert config.task_id == "test_insert"
        assert config.task_type.value == "direct_insert"
