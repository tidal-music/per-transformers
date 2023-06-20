import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c

from pyspark.sql.window import Window
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
from tidal_per_transformers.transformers.utils.dither_utils import dither


class TopItemsDitheringTransformer(LoggableTransformer):
    """
    Take the top 'n' records from a partition ordered by 'order_by' after applying dithering on the original
    relevance ordering.
    """
    def __init__(self, partition, order_by, n, epsilon=2.0, start_pos=0, keep_rank_col=False):
        super(TopItemsDitheringTransformer, self).__init__()
        self.partition = partition
        self.order_by = order_by
        self.n = n
        self.epsilon = epsilon
        self.start_pos = start_pos
        self.keep_rank_col = keep_rank_col

    def _transform(self, dataset):
        window = Window.partitionBy(self.partition).orderBy(self.order_by)
        ranked = dataset.withColumn(c.RN, F.row_number().over(window))
        dithered = dither(ranked, c.RN, self.partition, self.epsilon, self.start_pos).where(f"{c.RN} <= {self.n}")

        if self.keep_rank_col:
            return dithered
        else:
            return dithered.drop(c.RN)
