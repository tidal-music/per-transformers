from pyspark.sql import functions as F, DataFrame, Window
from tidal_per_transformers.transformers.utils import constants as c
from tidal_per_transformers.transformers import LoggableTransformer

import pyspark.sql.types as T


class GenericRecommendationFormatterTransformer(LoggableTransformer):
    """
    Transformer to add standard format to a set of recommendations (e.g. userId, [recommendedIds])
    with a template that will specify the content type, the module and its version.
    """

    def __init__(self,
                 group_key: str,
                 recommendations_col: str,
                 item_type: str,
                 module_name: str):
        super(GenericRecommendationFormatterTransformer, self).__init__()
        self.group_key = group_key
        self.recommendations_col = recommendations_col
        self.item_type = item_type.upper()
        self.module_name = module_name
        assert self.item_type in [x.upper() for x in [c.ALBUM, c.ARTIST, c.PLAYLIST, c.TRACK, c.MIX, c.RADIO,
                                                      "ARTIST_TOP_TRACKS", "MASTER_BUNDLE_ID", "TRACK_GROUP"]]

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .withColumn(c.TYPE, F.lit(self.item_type))
                .withColumn(c.MODULE_ID, F.lit(self.module_name))
                .select(F.col(self.group_key).cast(T.StringType()).alias(c.HASH_KEY), c.TYPE, c.MODULE_ID,
                        F.posexplode(self.recommendations_col))
                .withColumn(self.recommendations_col,
                            F.collect_list(F.struct(F.col(c.TYPE), F.col(c.COL).cast(T.StringType()).alias(c.ID))).over(
                                Window.partitionBy(c.HASH_KEY).orderBy(c.POS)))
                .groupby([c.HASH_KEY, c.MODULE_ID])
                .agg(F.max(self.recommendations_col).alias(self.recommendations_col))
                )
