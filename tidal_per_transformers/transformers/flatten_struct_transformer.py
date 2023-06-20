from typing import Dict
import pyspark.sql.functions as F

from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class FlattenStructTransformer(LoggableTransformer):
    """Flatten each struct field into a new column

    :param struct_fields: Dictionary containing new column names and their corresponding struct field names
    """
    def __init__(self, struct_fields: Dict[str, str]):
        super().__init__()
        self.struct_fields = struct_fields

    def _transform(self, dataset: DataFrame) -> DataFrame:
        for key, value in self.struct_fields.items():
            dataset = dataset.withColumn(key, F.col(value))
        return dataset
