class TDCError(Exception):
    """TDC基础异常"""
    pass


class ConfigError(TDCError):
    """配置错误基类"""
    pass


class ConfigInheritanceError(ConfigError):
    """配置继承错误"""
    pass


class ConfigCircularDependencyError(ConfigInheritanceError):
    """配置循环依赖错误"""
    pass


class SecretResolutionError(ConfigError):
    """密钥解析错误"""
    pass


class PipelineError(TDCError):
    """管道执行错误"""
    def __init__(self, message, step_id=None):
        super().__init__(message)
        self.step_id = step_id


class HTTPError(TDCError):
    """HTTP调用错误"""
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class StorageError(TDCError):
    """存储层错误"""
    pass


class GatewayAuthError(PipelineError):
    """网关认证错误"""
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class TokenExtractionError(PipelineError):
    """Token 提取错误"""
    pass


class UserSourceError(PipelineError):
    """用户来源错误"""
    pass
