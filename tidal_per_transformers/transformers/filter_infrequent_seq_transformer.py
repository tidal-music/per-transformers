from typing import Optional

import pyspark.sql.functions as F
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
from pyspark.sql.types import DataType

import tidal_per_transformers.transformers.utils.constants as c


class FilterInfrequentSeqItemsTransformer(LoggableTransformer):

    def __init__(self,
                 min_item_frequency: int,
                 sequence_column: str,
                 item_column: Optional[str],
                 sequence_id_column: str,
                 sequence_schema: DataType):
        """
        Filter out less frequent tracks from each playlist

        :param min_item_frequency:      Minimum number of sequences containing the item
        :param sequence_column:         Column name of the sequence, e.g., "tracks"
        :param item_column:             Column name of item field in the sequence (assume tracks are structs)
        :param sequence_id_column:      Column name of playlist id
        :param sequence_schema:         Data type of tracks column
        """
        super().__init__()
        self.min_item_frequency = min_item_frequency
        self.sequence_column = sequence_column
        self.item_column = item_column
        self.sequence_id_column = sequence_id_column
        self.sequence_schema = sequence_schema

    def _transform(self, dataset):
        explode_col = f"{self.sequence_column}.{self.item_column}" if self.item_column else self.sequence_column

        items = (dataset
                 .select(F.explode(F.col(explode_col)).alias(c.ITEM), F.col(self.sequence_id_column))
                 .groupBy(c.ITEM)
                 .agg(F.count(self.sequence_id_column).alias(c.COUNT))
                 .where(F.col(c.COUNT) >= self.min_item_frequency)
                 .select(c.ITEM)).toPandas()[c.ITEM].tolist()

        items_bc = dataset.sql_ctx.sparkSession.sparkContext.broadcast(set(items))

        @F.udf(returnType=self.sequence_schema)
        def filter_tracks(sequence):
            if self.item_column:
                return [item for item in sequence if item[self.item_column] in items_bc.value]
            else:
                return [item for item in sequence if item in items_bc.value]

        return dataset.withColumn(self.sequence_column, filter_tracks(self.sequence_column))
