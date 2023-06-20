from pyspark.sql import functions as F, DataFrame
from tidal_per_transformers.transformers import LoggableTransformer

import tidal_per_transformers.transformers.utils.constants as c


class ArtistCompoundMappingTransformer(LoggableTransformer):
    """
    Map the artist compound id's (e.g. Miguel feat. Travis Scott -> Miguel, Travis Scott)

    TODO: Find a better way of picking the top n featuring artist per compound id

    :returns: DataFrame where the compound id's each have been mapped to a separate row. E.g. when using this on the
              playback log you will get one log entry per artist.
    """
    def __init__(self, max_featuring_artists=3, keep_info=False, compound_artist_mapping: DataFrame = None):
        super(ArtistCompoundMappingTransformer, self).__init__()
        self.max_featuring_artists = max_featuring_artists
        self.keep_info = keep_info
        self.compound_artist_mapping = compound_artist_mapping

    def _transform(self, dataset):
        compound_map = (self.compound_artist_mapping
                        .withColumnRenamed(c.ARTIST_COMPOUND_ID, c.RESOLVED_ARTIST_ID)
                        .drop(c.ID))

        # If there is no compound entry we already have the main artist
        joined = (dataset
                  .join(compound_map, c.ARTIST_ID, "left")
                  .withColumn(c.PRIORITY, F.when(F.col(c.PRIORITY).isNull(), F.lit(0)).otherwise(F.col(c.PRIORITY)))
                  .where(F.col(c.PRIORITY) <= self.max_featuring_artists))

        mapped = (joined
                  .withColumn(c.ARTIST_ID, F.when(F.col(c.RESOLVED_ARTIST_ID).isNotNull(),
                                                  F.col(c.RESOLVED_ARTIST_ID)).otherwise(F.col(c.ARTIST_ID)))
                  .withColumn(c.MAIN_ARTIST, F.when(F.col(c.MAIN_ARTIST) == "true", F.lit(1)).otherwise(F.lit(0)))
            .drop(c.RESOLVED_ARTIST_ID, c.ARTIST_COMPOUND_ID))

        if not self.keep_info:
            mapped = mapped.drop(c.PRIORITY, c.MAIN_ARTIST)

        return mapped
