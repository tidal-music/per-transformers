from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class WithColumnRenamedTransformer(LoggableTransformer):
    """Transformer to rename a column

    """

    def __init__(self, source_column_name: str, target_column_name: str):
        super().__init__()
        self.source_column_name = source_column_name
        self.target_column_name = target_column_name

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.withColumnRenamed(self.source_column_name, self.target_column_name)
