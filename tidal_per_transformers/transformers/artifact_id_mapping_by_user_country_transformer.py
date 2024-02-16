from typing import Optional, List

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers import LoggableTransformer


class ArtifactIdMappingByUserCountryTransformer(LoggableTransformer):
    """
    The transformer maps trackGroup / masterAlbumId to the minimum productId / albumId
    that is available in the country of given user
    """
    def __init__(self,
                 item_metadata: DataFrame,
                 user_countries: DataFrame,
                 available_countries_col: str = c.AVAILABLE_COUNTRY_CODES,
                 user_country_col: str = c.COUNTRY_CODE,
                 id_col: str = c.ID,
                 item_id_col: str = c.TRACK_GROUP,
                 item_instance_id_col: str = c.PRODUCT_ID,
                 partition_cols: Optional[List[str]] = None):

        super().__init__()
        self.item_metadata = item_metadata.select(item_id_col,
                                                  F.col(id_col).alias(item_instance_id_col),
                                                  available_countries_col)
        self.user_countries = user_countries.select(c.USER_ID, user_country_col)
        self.item_id_col = item_id_col
        self.item_instance_id_col = item_instance_id_col
        self.available_countries_col = available_countries_col
        self.user_country_col = user_country_col
        self.partition_cols = partition_cols if partition_cols else [item_id_col, c.USER_ID]

    def _transform(self, dataset: DataFrame) -> DataFrame:
        window = Window.partitionBy(self.partition_cols).orderBy(self.item_instance_id_col)
        return (dataset
                .join(self.user_countries, c.USER_ID)
                .join(self.item_metadata, self.item_id_col)
                .where(F.array_contains(self.available_countries_col, F.col(self.user_country_col)))
                .withColumn(c.RN, F.rank().over(window))
                .where(F.col(c.RN) == 1)
                .drop(self.user_country_col, self.available_countries_col, c.RN))
