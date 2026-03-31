import pytest
from tdc.core.models import Context, PipelineResult
from tdc.core.constants import TaskType, TaskStatus, AuthType
from tdc.core.exceptions import TDCError, ConfigError, PipelineError


class TestContext:
    def test_context_set_and_get(self):
        ctx = Context(task_id="test_task", run_id="run_001")
        ctx.set("user_id", "12345")
        assert ctx.get("user_id") == "12345"

    def test_context_get_missing_with_default(self):
        ctx = Context(task_id="test_task", run_id="run_001")
        assert ctx.get("missing", "default") == "default"

    def test_context_to_dict(self):
        ctx = Context(task_id="test_task", run_id="run_001")
        ctx.set("a", 1)
        ctx.set("b", 2)
        result = ctx.to_dict()
        assert result == {"a": 1, "b": 2}


class TestTaskType:
    def test_task_type_values(self):
        assert TaskType.HTTP_SOURCE.value == "http_source"
        assert TaskType.DIRECT_INSERT.value == "direct_insert"


class TestTaskStatus:
    def test_task_status_values(self):
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.SUCCESS.value == "success"


class TestAuthType:
    def test_auth_type_values(self):
        assert AuthType.NONE.value == "none"
        assert AuthType.BEARER.value == "bearer"


class TestExceptions:
    def test_tdc_error(self):
        err = TDCError("test error")
        assert str(err) == "test error"

    def test_pipeline_error_with_step_id(self):
        err = PipelineError("step failed", step_id="step_1")
        assert str(err) == "step failed"
        assert err.step_id == "step_1"
