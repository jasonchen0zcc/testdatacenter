"""Execution statistics tracking."""
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class ExecutionStats:
    """Task execution statistics."""
    total: int = 0
    completed: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)

    def add_result(self, iteration: int, success: bool, error: Optional[str] = None):
        """Record single iteration result."""
        self.completed += 1
        if success:
            self.success += 1
        else:
            self.failed += 1
            if error:
                self.errors.append(f"iter {iteration}: {error}")

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.completed == 0:
            return 0.0
        return self.success / self.completed

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "total": self.total,
            "completed": self.completed,
            "success": self.success,
            "failed": self.failed,
            "skipped": self.skipped,
            "success_rate": f"{self.success_rate:.2%}",
            "errors": self.errors[:10]  # Limit errors in output
        }
