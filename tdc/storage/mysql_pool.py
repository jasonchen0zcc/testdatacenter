from typing import Dict
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, async_sessionmaker

from tdc.config.models import DBConfig


class MySQLPoolManager:
    """MySQL连接池管理器（支持多实例）"""

    def __init__(self):
        self.pools: Dict[str, AsyncEngine] = {}
        self.session_makers: Dict[str, async_sessionmaker] = {}

    def register(self, instance_id: str, dsn: str, pool_size: int = 10):
        """注册数据库实例"""
        engine = create_async_engine(
            dsn,
            pool_size=pool_size,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False
        )
        self.pools[instance_id] = engine
        self.session_makers[instance_id] = async_sessionmaker(engine, expire_on_commit=False)

    def register_from_config(self, config: DBConfig):
        """从配置批量注册"""
        for instance_id, instance_config in config.instances.items():
            dsn = f"mysql+aiomysql://{instance_config.user}:{instance_config.password}@{instance_config.host}:{instance_config.port}"
            self.register(instance_id, dsn, instance_config.pool_size)

    def get_engine(self, instance_id: str) -> AsyncEngine:
        """获取数据库引擎"""
        if instance_id not in self.pools:
            raise KeyError(f"Database instance not found: {instance_id}")
        return self.pools[instance_id]

    def get_session_maker(self, instance_id: str) -> async_sessionmaker:
        """获取会话构造器"""
        if instance_id not in self.session_makers:
            raise KeyError(f"Database instance not found: {instance_id}")
        return self.session_makers[instance_id]

    async def close_all(self):
        """关闭所有连接池"""
        for engine in self.pools.values():
            await engine.dispose()
