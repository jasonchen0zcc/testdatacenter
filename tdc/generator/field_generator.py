import random
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from faker import Faker

from tdc.config.models import FieldGeneratorConfig


class FieldGenerator(ABC):
    """字段生成器基类"""

    @abstractmethod
    def generate(self) -> Any:
        pass


class FakerGenerator(FieldGenerator):
    """Faker假数据生成器"""

    def __init__(self, config: FieldGeneratorConfig):
        self.faker = Faker(config.locale)
        self.generator_name = config.generator

    def generate(self) -> Any:
        generator = getattr(self.faker, self.generator_name)
        return generator()


class ChoiceGenerator(FieldGenerator):
    """随机选择生成器"""

    def __init__(self, config: FieldGeneratorConfig):
        self.values = config.values
        self.weights = config.weights

    def generate(self) -> Any:
        if self.weights:
            return random.choices(self.values, weights=self.weights, k=1)[0]
        return random.choice(self.values)


class SequenceGenerator(FieldGenerator):
    """序列生成器"""

    def __init__(self, config: FieldGeneratorConfig):
        self.current = config.start or 1
        self.step = config.step or 1

    def generate(self) -> Any:
        value = self.current
        self.current += self.step
        return value


class FunctionGenerator(FieldGenerator):
    """函数表达式生成器，通过 expr 执行自定义 Python 表达式"""

    @staticmethod
    def _msisdn_cn() -> str:
        """生成中国大陆有效手机号"""
        prefixes = [
            130, 131, 132, 133, 134, 135, 136, 137, 138, 139,
            145, 147, 149, 150, 151, 152, 153, 155, 156, 157,
            158, 159, 166, 170, 171, 172, 173, 175, 176, 177,
            178, 180, 181, 182, 183, 184, 185, 186, 187, 188,
            189, 190, 191, 192, 193, 195, 196, 197, 198, 199,
        ]
        prefix = random.choice(prefixes)
        return f"{prefix}{random.randint(0, 99999999):08d}"

    def __init__(self, config: FieldGeneratorConfig):
        self.expr = config.expr
        self.locale = config.locale or "zh_CN"
        self.faker = Faker(self.locale)
        self.globals = {
            "__builtins__": {
                "str": str,
                "int": int,
                "float": float,
                "bool": bool,
                "len": len,
                "range": range,
                "abs": abs,
                "min": min,
                "max": max,
                "sum": sum,
            },
        }
        self.locals = {
            "faker": self.faker,
            "random": random,
            "uuid": uuid,
            "datetime": datetime,
            "msisdn_cn": self._msisdn_cn,
        }

    def generate(self) -> Any:
        return eval(self.expr, self.globals, self.locals)


class FieldGeneratorFactory:
    """字段生成器工厂"""

    @staticmethod
    def create(config: FieldGeneratorConfig) -> FieldGenerator:
        if config.type == "faker":
            return FakerGenerator(config)
        elif config.type == "choice":
            return ChoiceGenerator(config)
        elif config.type == "sequence":
            return SequenceGenerator(config)
        elif config.type == "function":
            return FunctionGenerator(config)
        else:
            raise ValueError(f"Unknown generator type: {config.type}")
