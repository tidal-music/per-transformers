from pyspark.sql import DataFrame

from tidal_per_transformers.transformers import LoggableTransformer


class DistinctTransformer(LoggableTransformer):
    """Transformer to get distinct rows
    """
    def __init__(self):
        super().__init__()

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.distinct()
