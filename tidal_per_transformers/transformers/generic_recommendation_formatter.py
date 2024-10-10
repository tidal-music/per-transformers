from pyspark.sql import functions as F, DataFrame
from tidal_per_transformers.transformers.utils import constants as c
from tidal_per_transformers.transformers import LoggableTransformer


class GenericRecommendationFormatter(LoggableTransformer):
    """

    """

    def __init__(self,
                 hash_key: str,
                 recommendations_col: str,
                 item_type: str,
                 module_name: str,
                 module_version: str):
        super(GenericRecommendationFormatter, self).__init__()
        self.hash_key = hash_key
        self.recommendations_col = recommendations_col
        self.item_type = item_type
        self.module_name = module_name
        self.module_version = module_version

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .withColumn(c.TYPE, F.lit(self.item_type))
                .withColumn(c.MODULE_ID, F.lit(f"{self.module_name}_v{self.module_version}"))
                .withColumn(c.ID, F.explode(self.recommendations_col))
                .groupby([F.col(self.hash_key).alias(c.HASH_KEY), c.MODULE_ID])
                .agg(F.collect_list(F.struct(F.col(c.TYPE), F.col(c.ID))).alias(self.recommendations_col))
                )

