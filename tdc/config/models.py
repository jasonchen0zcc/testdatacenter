from typing import Any, Dict, List, Literal, Optional
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


class AssertionConfig(BaseModel):
    """粗粒度断言配置"""
    # HTTP 状态码断言（支持单个值或列表）
    status_code: Optional[Any] = None  # 200 或 [200, 201]
    # JSON 字段断言
    json_path: Optional[str] = None           # 字段路径，如 "code"
    json_expected: Optional[Any] = None       # 期望值，如 200
    # JSON 布尔成功标识断言
    json_success_path: Optional[str] = None   # 布尔字段路径，如 "success"
    json_success_value: bool = True           # 预期值，默认为 true


class PipelineStepConfig(BaseModel):
    step_id: str
    name: Optional[str] = None
    condition: Optional[str] = None
    http: HTTPConfig
    extract: Dict[str, str] = Field(default_factory=dict)
    assertions: Optional[AssertionConfig] = None  # 粗粒度断言配置


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


class GatewayConfig(BaseModel):
    """网关认证配置"""
    auth_url: str
    method: str = "POST"
    body_template: str
    token_path: str = "data.access_token"  # JSONPath
    header_name: str = "Authorization"
    header_prefix: str = "Bearer "
    headers: Dict[str, str] = Field(default_factory=dict)


class UserHttpConfig(BaseModel):
    """HTTP 用户来源配置"""
    url: str
    method: str = "GET"
    headers: Dict[str, str] = Field(default_factory=dict)
    body: Optional[str] = None  # JSON 请求体（POST 时使用）
    user_path: str = "data"  # JSONPath 提取用户列表或单个用户
    user_field: Optional[str] = None  # 从用户对象中提取字段
    single_user: bool = False  # 返回的是单个用户而非列表


class ExecutionConfig(BaseModel):
    """批量执行配置"""
    iterations: int = 1
    user_source: Literal["faker", "http", "list"] = "faker"
    # faker 模式
    user_template: Optional[str] = "{{ faker.username }}"
    # http 模式
    user_http: Optional[UserHttpConfig] = None
    # list 模式
    user_list: Optional[List[str]] = None
    delay_ms: int = 0  # 每次迭代延迟（毫秒）
    # 并发控制
    concurrency: int = 1  # 并发数，默认1（串行）
    batch_delay_ms: int = 0  # 每批完成后延迟（毫秒）
    fail_fast: bool = False  # true=任一失败立即停止
    continue_on_error: bool = True  # 单迭代失败是否继续下一迭代


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
    # 新增：网关认证和批量执行配置
    gateway: Optional[GatewayConfig] = None
    execution: Optional[ExecutionConfig] = None
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
