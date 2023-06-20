import pyspark.sql.functions as F
from pyspark.sql.window import Window
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class TopItemsTransformer(LoggableTransformer):
    """
    Take the top 'n' records from a partition ordered by 'order_by'
    :param partition:   The columns used to partition the DataFrame
    :param order_by:    The column(s) used to sort the partitions
    :param n:           Number of top items to keep per partition
    :param ranking_function: Ranking function, e.g. "F.dense_rank" to account for ties. Default is ranking by rows, ignoring ties.
    :param keep_rnk_col: Option to keep or drop rank column

    """
    def __init__(self, partition, order_by, n, ranking_function=F.row_number, keep_rnk_col=False):
        super(TopItemsTransformer, self).__init__()
        self.partition = partition
        self.order_by = order_by
        self.n = n
        self.ranking_function = ranking_function
        self.keep_rnk_col = keep_rnk_col

    def _transform(self, dataset):
        window = Window.partitionBy(self.partition).orderBy(self.order_by)
        output = dataset.withColumn("rn", self.ranking_function().over(window)).where(F.col("rn") <= self.n)
        if not self.keep_rnk_col:
            output = output.drop("rn")
        return output
