import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
from math import pow

from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
import tidal_per_transformers.transformers.utils.constants as c


@F.udf(returnType=T.DoubleType())
def exponential_decay_udf(days_since: int,
                          period_length: int,
                          decay_rate: float):
    return pow(decay_rate, float(days_since) / period_length)


class ScoreNormalizerTransformer(LoggableTransformer):
    """
    Normalize scores by applying exponential decay to the top scores. This is done per partition.
    """
    def __init__(self,
                 partition_by: str,
                 order_by: str,
                 decay_factor: float = 0.05,
                 cutoff: int = 250):
        super(ScoreNormalizerTransformer, self).__init__()
        self.partition_by = partition_by
        self.order_by = order_by
        self.decay_factor = decay_factor
        self.cutoff = cutoff

    def _transform(self, dataset):
        window = Window.partitionBy(self.partition_by).orderBy(F.desc(self.order_by))
        return (dataset
                .withColumn("row_number", F.dense_rank().over(window)-F.lit(1))
                .where(F.col("row_number") <= self.cutoff)
                .withColumn("max_row_number", F.max("row_number").over(Window.partitionBy(self.partition_by)) + F.lit(1))
                .withColumn(self.order_by, exponential_decay_udf(F.col("row_number"),
                                                                 F.col("max_row_number"),
                                                                 F.lit(self.decay_factor)))
                .select(self.partition_by, c.ITEM, self.order_by))

