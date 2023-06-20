from typing import List

from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class DropDuplicatesTransformer(LoggableTransformer):
    """Transformer to drop duplicates

    :param columns: Columns to drop duplicates on
    """

    def __init__(self, columns: List[str] = None):
        super().__init__()
        self.columns = columns

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.dropDuplicates(self.columns)
