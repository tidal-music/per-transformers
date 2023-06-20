import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c
from pyspark.sql import DataFrame

from tidal_per_transformers.transformers import LoggableTransformer


class BroadcastArrayIntersectTransformer(LoggableTransformer):
    def __init__(self, array_col: str, items_to_keep: DataFrame):
        """
        Transformer to filter out invalid items from an array column based on items_to_keep.
        :param array_col:       name of column containing an array of items
        :param items_to_keep:   a dataframe with only one row containing the list of items to be kept
        """
        super().__init__()
        self.array_col = array_col
        self.items_to_keep = items_to_keep.select(c.ITEMS_TO_KEEP)

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .join(F.broadcast(self.items_to_keep))
                .withColumn(self.array_col, F.array_intersect(self.array_col, c.ITEMS_TO_KEEP))
                .drop(c.ITEMS_TO_KEEP))
