from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import uuid4


@dataclass
class Context:
    """管道执行上下文"""
    task_id: str
    run_id: str = field(default_factory=lambda: str(uuid4())[:8])
    _data: Dict[str, Any] = field(default_factory=dict)

    def set(self, key: str, value: Any) -> None:
        self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def to_dict(self) -> Dict[str, Any]:
        return self._data.copy()


@dataclass
class PipelineResult:
    """管道执行结果"""
    context: Context
    success: bool = True
    error: Optional[str] = None
    step_results: list = field(default_factory=list)
