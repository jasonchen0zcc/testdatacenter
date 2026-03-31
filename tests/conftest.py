import pytest
from unittest.mock import MagicMock, AsyncMock


@pytest.fixture
def mock_db_session():
    return AsyncMock()


@pytest.fixture
def sample_task_config():
    return {
        "task_id": "test_task",
        "task_name": "Test Task",
        "task_type": "http_source",
        "schedule": "0 * * * *",
        "pipeline": [
            {
                "step_id": "step1",
                "http": {
                    "url": "https://api.test.com/user",
                    "method": "GET"
                },
                "extract": {"user_id": "$.data.id"}
            }
        ],
        "tag_mapping": {
            "user_id": "{{ context.user_id }}",
            "order_id": "N/A",
            "data_tag": "TEST_DATA"
        },
        "target_db": {
            "instance": "test_db",
            "database": "test"
        }
    }
