from typing import List

from pyspark.ml.base import Transformer
from pyspark.sql import DataFrame


class DropColumnsTransformer(Transformer):
    def __init__(self, columns: List[str]):
        self.columns = columns
        assert isinstance(self.columns, List), "columns should be a list"
        super(DropColumnsTransformer, self).__init__()

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return dataset.drop(*self.columns)
