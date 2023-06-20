from pyspark.sql import DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers import LoggableTransformer


class MultipleCompoundTransformer(LoggableTransformer):
    """
    Custom compound mapping transformer mapping compoundIds to all main artists on a track
    For tracks with > 1 main artists they are all assigned a weight equal to count/num_artists
    E.g. for each stream on a track featuring both Jay-Z and Kanye they each receive 0.5 streams each
    """
    def __init__(self, compound_mapping: DataFrame):
        super().__init__()
        self.compound_mapping = compound_mapping

    def _transform(self, dataset) -> DataFrame:
        main_artist_mapping = self.get_main_artist_counts()

        return (dataset
                .join(main_artist_mapping, c.ARTIST_ID, "left")
                .withColumn(c.COUNT, F.when(F.col(c.ARTIST_COMPOUND_ID).isNull(),
                                            F.col(c.COUNT)).otherwise(F.col(c.COUNT) / F.col(c.NUM_MAIN_ARTISTS)))
                .withColumn(c.ARTIST_ID, F.when(F.col(c.ARTIST_COMPOUND_ID).isNull(),
                                                F.col(c.ARTIST_ID)).otherwise(F.col(c.ARTIST_COMPOUND_ID)))
                .drop(c.ARTIST_COMPOUND_ID, c.NUM_MAIN_ARTISTS))

    def get_main_artist_counts(self) -> DataFrame:
        artist_window = Window.partitionBy(c.ARTIST_ID)

        return (self.compound_mapping
                .where(F.col(c.MAIN_ARTIST))
                .select(c.ARTIST_ID, c.ARTIST_COMPOUND_ID)
                .withColumn(c.NUM_MAIN_ARTISTS, F.count("*").over(artist_window)))