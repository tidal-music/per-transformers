from typing import Any

from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class WithColumnTransformer(LoggableTransformer):
    """Create a new column with a given pyspark expression.

    """

    def __init__(self, column_name: str, expression: Any):
        super().__init__()
        self.column_name = column_name
        self.expression = expression

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumn(self.column_name, self.expression)
