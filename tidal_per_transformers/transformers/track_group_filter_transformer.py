import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class TrackGroupFilterTransformer(LoggableTransformer):
    """
    Transformer for removing track groups based on either category, popularity or a combination of the two. E.g. if you
    want to remove Christmas and children music from a track based model or mix you can use this transformer to easily
    exclude those content types. The popularity features are based on streaming data from the last 12 months
    """

    def __init__(self,
                 track_filters: DataFrame,
                 remove_children_music: bool,
                 remove_ambient_music: bool,
                 remove_holiday_music: bool,
                 min_track_streams: int = 500,
                 min_track_streamers: int = 200,
                 min_track_duration: int = 45,
                 max_track_duration: int = 1200):
        """
        :param track_filters:           DataFrame containing the content filter information
        :param remove_ambient_music:    flag for toggling ambient music on/off
        :param remove_holiday_music:    flag for toggling holiday music on/off
        :param remove_children_music:   flag for toggling children music on/off
        :param min_track_streams:       minimum number of recorded streams
        :param min_track_streamers:     minimum number of unique listeners
        :param min_track_duration:      minimum allowed duration of a track
        :param max_track_duration:      maximum allowed duration of a track
        """
        super(TrackGroupFilterTransformer, self).__init__()
        self.track_filters = track_filters
        self.remove_children_music = remove_children_music
        self.remove_ambient_music = remove_ambient_music
        self.remove_holiday_music = remove_holiday_music
        self.min_track_streams = min_track_streams
        self.min_track_streamers = min_track_streamers
        self.min_track_duration = min_track_duration
        self.max_track_duration = max_track_duration

    def _transform(self, dataset):
        assert c.TRACK_GROUP in dataset.columns
        return self.filter_tracks(dataset)

    def filter_tracks(self, tracks: DataFrame) -> DataFrame:
        cleaned_tracks = self.apply_filters(self.track_filters,
                                            self.min_track_streams,
                                            self.min_track_streamers,
                                            self.min_track_duration,
                                            self.max_track_duration,
                                            self.remove_holiday_music,
                                            self.remove_ambient_music,
                                            self.remove_children_music)

        return tracks.join(cleaned_tracks, c.TRACK_GROUP)

    @staticmethod
    def apply_filters(category_filters: DataFrame,
                      stream_count: int,
                      streamers_count: int,
                      min_track_duration: int,
                      max_track_duration: int,
                      drop_holiday: bool,
                      drop_ambient: bool,
                      drop_children: bool):
        return apply_category_filters(dataframe=category_filters
                                      .where(F.col(c.AVAILABLE))
                                      .where(F.col(c.NON_MUSIC) == 0)
                                      .where(F.col(c.STREAM_COUNT) >= stream_count)
                                      .where(F.col(c.STREAMERS_COUNT) >= streamers_count)
                                      .where(F.col(c.DURATION).between(min_track_duration, max_track_duration)),
                                      drop_holiday=drop_holiday,
                                      drop_ambient=drop_ambient,
                                      drop_children=drop_children).select(c.TRACK_GROUP)


def apply_category_filters(dataframe: DataFrame, drop_holiday: bool, drop_ambient: bool, drop_children: bool):
    if drop_children:
        dataframe = dataframe.where(F.col(c.CHILDREN) == 0)

    if drop_ambient:
        dataframe = dataframe.where(F.col(c.AMBIENT) == 0)

    if drop_holiday:
        dataframe = dataframe.where(F.col(c.HOLIDAY) == 0)

    return dataframe
