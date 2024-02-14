from typing import Optional, List

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers import LoggableTransformer


class TrackGroupToProductIdMappingByUserCountryTransformer(LoggableTransformer):
    """
    The transformer maps track group to the minimum product id that is available in the country of given user
    """
    def __init__(self,
                 track_metadata: DataFrame,
                 user_countries: DataFrame,
                 available_countries_col: str = c.AVAILABLE_COUNTRY_CODES,
                 user_country_col: str = c.COUNTRY_CODE,
                 track_id_col: str = c.ID,
                 partition_cols: Optional[List[str]] = None):

        super().__init__()
        self.track_metadata = track_metadata.select(c.TRACK_GROUP,
                                                    F.col(track_id_col).alias(c.PRODUCT_ID),
                                                    available_countries_col)
        self.user_countries = user_countries.select(c.USER_ID, user_country_col)
        self.available_countries_col = available_countries_col
        self.user_country_col = user_country_col
        self.partition_cols = partition_cols if partition_cols else [c.TRACK_GROUP, c.USER_ID]

    def _transform(self, dataset: DataFrame) -> DataFrame:
        window = Window.partitionBy(self.partition_cols).orderBy(c.PRODUCT_ID)
        return (dataset
                .join(self.user_countries, c.USER_ID)
                .join(self.track_metadata, c.TRACK_GROUP)
                .where(F.array_contains(self.available_countries_col, F.col(self.user_country_col)))
                .withColumn(c.RN, F.rank().over(window))
                .where(F.col(c.RN) == 1)
                .drop(self.user_country_col, self.available_countries_col, c.RN))
