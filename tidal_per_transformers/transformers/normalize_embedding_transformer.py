from typing import Optional

import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from tidal_per_transformers.transformers import LoggableTransformer


class NormalizeEmbeddingTransformer(LoggableTransformer):

    def __init__(self, col: str, order: Optional = None):
        """
        Normalize the embedding vector in the column 'col' by dividing it by the vector norm

        :param col: The column containing the embedding vector
        :param order: The order of the norm. If None, the L2 norm is used
        """
        super().__init__()
        self.col = col
        self.order = order

    def _transform(self, data: DataFrame) -> DataFrame:
        @F.udf(returnType=T.ArrayType(T.FloatType()))
        def norm_udf(x):
            return [float(i) for i in x / np.linalg.norm(x, self.order)]

        return data.withColumn(self.col, norm_udf(self.col))