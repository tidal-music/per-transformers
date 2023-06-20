from typing import Optional

import pyspark.sql.functions as F

from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class BlazingTextInputFormatTransformer(LoggableTransformer):
    """ Convert an array column to the input format required by BlazingText """

    def __init__(self, items_column: str, alias: Optional[str] = None):
        """
        :param items_column: column containing the input sequences
        :param alias:        name given to the output column (optional)
        """
        super(BlazingTextInputFormatTransformer, self).__init__()
        self.items_column = items_column
        self.alias = alias if alias else items_column

    def _transform(self, dataset):
        return dataset.select(F.array_join(F.col(self.items_column), " ").alias(self.alias))

