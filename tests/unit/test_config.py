from tdc.config.models import TaskConfig


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
                    "http": {"url": "https://api.example.com/test", "method": "GET"},
                }
            ],
            "tag_mapping": {
                "user_id": "$.data.id",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
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
                "fields": {"id": {"type": "faker", "generator": "uuid4"}},
            },
            "tag_mapping": {
                "user_id": "{{ faker.uuid4 }}",
                "order_id": "N/A",
                "data_tag": "TEST",
            },
            "target_db": {"instance": "test_db", "database": "test"},
        }
        config = TaskConfig(**data)
        assert config.task_id == "test_insert"
        assert config.task_type.value == "direct_insert"

    def test_pipeline_step_with_db_assertions(self):
        from tdc.config.models import DBAssertionConfig, DBAssertionMode, PipelineStepConfig, HTTPConfig

        step = PipelineStepConfig(
            step_id="check_db",
            http=HTTPConfig(url="http://example.com"),
            db_assertions=[
                DBAssertionConfig(
                    instance="user_db",
                    database="order_db",
                    mode=DBAssertionMode.TABLE,
                    table="orders",
                    where='order_no = "123"',
                    expected_rows=1,
                    delay_ms=100,
                )
            ],
        )
        assert step.db_assertions is not None
        assert len(step.db_assertions) == 1
        assert step.db_assertions[0].instance == "user_db"
