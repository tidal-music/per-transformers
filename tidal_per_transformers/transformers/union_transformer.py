from pyspark.sql.dataframe import DataFrame

from transformers.loggable_transformer import LoggableTransformer


class UnionTransformer(LoggableTransformer):
    def __init__(self, other: DataFrame):
        super().__init__()
        self.other = other

    def _transform(self, dataset):
        return dataset.union(self.other.select(dataset.columns))
