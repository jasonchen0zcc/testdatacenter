from enum import Enum


class TaskType(str, Enum):
    HTTP_SOURCE = "http_source"
    DIRECT_INSERT = "direct_insert"


class TaskStatus(str, Enum):
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class AuthType(str, Enum):
    NONE = "none"
    BASIC = "basic"
    BEARER = "bearer"
    HMAC = "hmac"
