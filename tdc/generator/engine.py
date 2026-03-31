from typing import List, Dict, Any

from tdc.config.models import DataTemplateConfig
from tdc.generator.field_generator import FieldGeneratorFactory


class DataGeneratorEngine:
    """数据生成引擎"""

    def __init__(self, config: DataTemplateConfig):
        self.config = config
        self.field_generators = {}
        for field_name, field_config in config.fields.items():
            self.field_generators[field_name] = FieldGeneratorFactory.create(field_config)

    def generate_batch(self, batch_size: int = None) -> List[Dict[str, Any]]:
        """生成一批数据"""
        size = batch_size or self.config.batch_size
        records = []
        for _ in range(size):
            record = self._generate_single()
            records.append(record)
        return records

    def generate_all(self) -> List[Dict[str, Any]]:
        """生成全部数据"""
        records = []
        remaining = self.config.total_count

        while remaining > 0:
            batch_size = min(self.config.batch_size, remaining)
            batch = self.generate_batch(batch_size)
            records.extend(batch)
            remaining -= batch_size

        return records

    def _generate_single(self) -> Dict[str, Any]:
        """生成单条记录"""
        record = {}
        for field_name, generator in self.field_generators.items():
            record[field_name] = generator.generate()
        return record
