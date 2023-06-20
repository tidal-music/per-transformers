from typing import Any

from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class JoinTransformer(LoggableTransformer):
    """Transformer to join to spark dataframes

    :param df: DataFrame to join with
    :param on: Join condition
    :param how: Join type
    """

    def __init__(self, df: DataFrame, on: Any, how: str = "inner", hint: str = None):
        super().__init__()
        self.df = df.hint(hint, on) if hint else df
        self.join_condition = on
        self.join_type = how

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.join(self.df, self.join_condition, self.join_type)
