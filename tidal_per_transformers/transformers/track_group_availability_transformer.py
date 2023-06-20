import pyspark.sql.functions as F
from pyspark.sql import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers import LoggableTransformer


class TrackGroupAvailabilityTransformer(LoggableTransformer):

    def __init__(self, track_group_availability: DataFrame):
        """
        Filter any track group from the input dataset that is not available for streaming. This transformer is for
        non-personalized use-cases where we just verify that a track is stream ready in at least one country

        :param track_group_availability:  all available track groups (source: track group availability job)
        """
        super(TrackGroupAvailabilityTransformer, self).__init__()
        self.track_group_availability = track_group_availability

    def _transform(self, dataset) -> DataFrame:
        available_track_groups = self.track_group_availability.select(c.TRACK_GROUP)
        return dataset.join(available_track_groups, c.TRACK_GROUP)


def get_available_track_groups(tracks_metadata: DataFrame):
    """
    Get track groups that are available in at least one country

    :param tracks_metadata:     tracks metadata df
    """
    return (tracks_metadata
            .where(F.size(c.AVAILABLE_COUNTRY_CODES) > 0)
            .select(c.TRACK_GROUP)
            .distinct())
