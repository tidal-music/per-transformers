from pyspark.sql import DataFrame

from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class OrderByTransformer(LoggableTransformer):
    def __init__(self, subset):
        super().__init__()
        self.subset = subset

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.orderBy(self.subset)
