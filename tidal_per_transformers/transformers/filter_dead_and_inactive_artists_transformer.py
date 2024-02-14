from datetime import datetime, timedelta

import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
import tidal_per_transformers.transformers.utils.constants as c

from tidal_per_transformers.transformers import LoggableTransformer

DEATH_DATE_BUFFER_DAYS = 180  # Time window before dead artists are removed
ACTIVE_YEARS_BUFFER = 1

METADATA_DEATH_DATE = "Death Date"
METADATA_ACTIVE_YEARS = "Active Years"


class FilterDeadAndInactiveArtistsTransformer(LoggableTransformer):
    """
    Filter out releases from artists which are either dead or no longer active
    """
    def __init__(self, rovi_metadata, artist_column=c.ARTIST_ID):
        super().__init__()
        self.artist_column = artist_column
        self.rovi_metadata = rovi_metadata

    # TODO: use artists feature store
    def _transform(self, dataset):
        metadata = (self.rovi_metadata
                    .where(F.col(c.TYPE).isin([METADATA_DEATH_DATE, METADATA_ACTIVE_YEARS]))
                    .withColumnRenamed(c.ARTIFACT_ID, self.artist_column)
                    .withColumnRenamed(c.TYPE, c.METADATA_TYPE)
                    .withColumnRenamed(c.VALUE, c.METADATA_VALUE))

        living_artists = self._filter_dead_artists(dataset, metadata, DEATH_DATE_BUFFER_DAYS)
        return self._filter_non_active_artists(living_artists, metadata, ACTIVE_YEARS_BUFFER)

    def _filter_dead_artists(self, dataset, metadata, buffer_days):
        cut_off = (datetime.today() - timedelta(days=buffer_days)).date()

        death_dates = metadata.where(F.col(c.METADATA_TYPE) == METADATA_DEATH_DATE)

        dead_artists = (death_dates
                        .filter(f"'{cut_off}' > {c.METADATA_VALUE}")
                        .select(F.col(self.artist_column)))

        return dataset.join(dead_artists, self.artist_column, "left_anti")

    def _filter_non_active_artists(self, dataset, metadata, delay_years):
        cut_off = (datetime.today() - relativedelta(years=delay_years)).date().year
        active_years = metadata.where(F.col(c.METADATA_TYPE) == METADATA_ACTIVE_YEARS)
        valid_entries = active_years.where(F.length(F.col(c.METADATA_VALUE)) == 21)

        last_active_year = (valid_entries
                            .withColumn("lastActiveYear", F.substring(F.split(c.METADATA_VALUE, "_")[1], 0, 4))
                            .groupBy(self.artist_column).agg(F.max("lastActiveYear").alias("max"))
                            .filter(F.col("max") <= cut_off)
                            .select(F.col(self.artist_column)))

        return dataset.join(last_active_year, self.artist_column, "left_anti")
