import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c

from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class TrackGroupAvailabilityByCountryTransformer(LoggableTransformer):

    def __init__(self,
                 track_group_availability: DataFrame,
                 country_col: str = c.COUNTRY_CODE,
                 availability_column: str = c.AVAILABLE_COUNTRY_CODES):
        """
        Filter any track group from the input dataset that is not available for streaming in the given country.
        This is used in personalized models to avoid recommending tracks that are not available to a user.

        :param track_group_availability:  all available track groups (source: track group availability job)
        :param country_col:               column containing the country code of the user
        :param availability_column:       column containing the country code the track is available in
        """
        super(TrackGroupAvailabilityByCountryTransformer, self).__init__()
        self.track_group_availability = track_group_availability
        self.country_col = country_col
        self.availability_column = availability_column

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .join(self.track_group_availability, c.TRACK_GROUP)
                .where(F.array_contains(self.availability_column, F.col(self.country_col)))
                .drop(self.availability_column))
