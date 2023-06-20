import pyspark.sql.types as T
from pyspark.sql import Window
from pyspark.sql import functions as F

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer

SEQUENCE_HASH = "sequenceHash"


class DistinctSequenceTransformer(LoggableTransformer):
    """
    Deduplicate a dataframe by a column containing a collection. This can e.g. be used to remove duplicate
    playlists from a dataframe. Order is ignored meaning that the lists [1, 2, 3] and [2, 1, 3] will be considered
    duplicates. For draws we randomly select a row and discard all others.

    :param sequence_col: column containing the sequence
    """
    def __init__(self, sequence_col: str):
        super().__init__()
        self.sequence_col = sequence_col

    def _transform(self, dataset):
        hashes = dataset.withColumn(SEQUENCE_HASH, sequence_hash(F.col(self.sequence_col)))
        window = Window.partitionBy(SEQUENCE_HASH).orderBy(F.rand())
        return (hashes
                .withColumn(c.RN, F.row_number().over(window))
                .where(F.col(c.RN) == 1)
                .drop(SEQUENCE_HASH, c.RN))


@F.udf(returnType=T.StringType())
def sequence_hash(sequence):
    return hash(tuple(sorted(sequence)))
