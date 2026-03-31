from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator

from tdc.core.constants import TaskType, AuthType


class HTTPAuthConfig(BaseModel):
    type: AuthType = AuthType.NONE
    token: Optional[str] = None
    secret_key: Optional[str] = None
    algorithm: str = "sha256"


class HTTPConfig(BaseModel):
    url: str
    method: str = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    body_template: Optional[str] = None
    timeout: int = 30
    auth: HTTPAuthConfig = Field(default_factory=HTTPAuthConfig)


class PipelineStepConfig(BaseModel):
    step_id: str
    name: Optional[str] = None
    condition: Optional[str] = None
    http: HTTPConfig
    extract: Dict[str, str] = Field(default_factory=dict)


class FieldGeneratorConfig(BaseModel):
    type: str  # faker, choice, sequence, function, reference
    generator: Optional[str] = None
    locale: str = "zh_CN"
    values: Optional[List[Any]] = None
    weights: Optional[List[float]] = None
    start: Optional[int] = None
    step: Optional[int] = None
    expr: Optional[str] = None
    ref: Optional[str] = None


class RelationConfig(BaseModel):
    table: str
    count: int = 1
    mapping: Dict[str, Any]


class DataTemplateConfig(BaseModel):
    table: str
    batch_size: int = 1000
    total_count: int = 1000
    fields: Dict[str, FieldGeneratorConfig]
    relations: Optional[List[RelationConfig]] = None


class TagMappingConfig(BaseModel):
    user_id: str
    order_id: str
    data_tag: str
    ext_info: Optional[Dict[str, Any]] = None


class TargetDBConfig(BaseModel):
    instance: str
    database: str
    sharding_key: Optional[str] = None
    sharding_count: Optional[int] = None


class RetryConfig(BaseModel):
    max_attempts: int = 3
    delay: int = 5
    backoff: str = "fixed"


class OnFailureConfig(BaseModel):
    action: str = "stop"  # stop, continue, retry
    retry: RetryConfig = Field(default_factory=RetryConfig)


class TaskConfig(BaseModel):
    task_id: str
    task_name: str
    task_type: TaskType
    schedule: str
    enabled: bool = True
    timeout: int = 300
    on_failure: OnFailureConfig = Field(default_factory=OnFailureConfig)
    # http_source specific
    pipeline: Optional[List[PipelineStepConfig]] = None
    tag_mapping: Optional[TagMappingConfig] = None
    # direct_insert specific
    data_template: Optional[DataTemplateConfig] = None
    # common
    target_db: TargetDBConfig

    @field_validator("pipeline")
    @classmethod
    def validate_pipeline_for_http_source(cls, v, info):
        values = info.data
        if values.get("task_type") == TaskType.HTTP_SOURCE and not v:
            raise ValueError("http_source tasks require pipeline configuration")
        return v

    @field_validator("data_template")
    @classmethod
    def validate_data_template_for_direct_insert(cls, v, info):
        values = info.data
        if values.get("task_type") == TaskType.DIRECT_INSERT and not v:
            raise ValueError("direct_insert tasks require data_template configuration")
        return v


class DBInstanceConfig(BaseModel):
    host: str
    port: int = 3306
    user: str
    password: str
    pool_size: int = 10


class DBConfig(BaseModel):
    instances: Dict[str, DBInstanceConfig]
