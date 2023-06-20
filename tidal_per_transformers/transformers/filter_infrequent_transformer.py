from pyspark.sql.window import Window
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
import pyspark.sql.functions as F


class FilterInfrequentTransformer(LoggableTransformer):
    """
    Remove the items with a count smaller than cutoff

    :param columns: The columns used to group the dataset
    :param cutoff:  The minimum allowed group size
    """
    def __init__(self, columns, cutoff):
        super(FilterInfrequentTransformer, self).__init__()
        self.columns = columns
        self.cutoff = cutoff

    def _transform(self, dataset):
        window_spec = Window.partitionBy(self.columns)

        return (dataset
            .withColumn("cnt", F.count("*").over(window_spec))
            .where(F.col("cnt") >= self.cutoff)
            .drop("cnt"))
