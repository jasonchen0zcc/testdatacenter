import random
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
        else:
            raise ValueError(f"Unknown generator type: {config.type}")
