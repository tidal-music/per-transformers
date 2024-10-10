from pyspark.sql import functions as F, DataFrame, Window
from tidal_per_transformers.transformers.utils import constants as c
from tidal_per_transformers.transformers import LoggableTransformer


class GenericRecommendationFormatter(LoggableTransformer):
    """
    Transformer to add standard format to a set of recommendations (e.g. userId, [recommendedIds])
    with a template that will specify the content type, the module and its version.
    """

    def __init__(self,
                 group_key: str,
                 recommendations_col: str,
                 item_type: str,
                 module_name: str,
                 module_version: str):
        super(GenericRecommendationFormatter, self).__init__()
        self.group_key = group_key
        self.recommendations_col = recommendations_col
        self.item_type = item_type
        self.module_name = module_name
        self.module_version = module_version

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .withColumn(c.TYPE, F.lit(self.item_type))
                .withColumn(c.MODULE_ID, F.lit(f"{self.module_name}_v{self.module_version}"))
                .select(F.col(self.group_key).alias(c.HASH_KEY), c.TYPE, c.MODULE_ID,
                        F.posexplode(self.recommendations_col))
                .withColumn(self.recommendations_col,
                            F.collect_list(F.struct(F.col(c.TYPE), F.col("col").alias(c.ID))).over(
                                Window.partitionBy(c.HASH_KEY).orderBy("pos")))
                .groupby([c.HASH_KEY, c.MODULE_ID])
                .agg(F.max(self.recommendations_col).alias(self.recommendations_col))
                )
