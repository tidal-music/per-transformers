from typing import Union, Dict

from pyspark.sql import DataFrame

from tidal_per_transformers.transformers import LoggableTransformer


class ReplaceNullValuesTransformer(LoggableTransformer):
    """Transformer to replace null values
    """
    def __init__(self, value: Union[object, Dict]):
        super().__init__()
        self.value = value

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.na.fill(self.value)
