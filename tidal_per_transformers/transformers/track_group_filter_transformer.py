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
        filtered_tracks = self.apply_invalid_filters(self.track_filters,
                                                     self.min_track_streams,
                                                     self.min_track_streamers,
                                                     self.min_track_duration,
                                                     self.max_track_duration,
                                                     self.remove_holiday_music,
                                                     self.remove_ambient_music,
                                                     self.remove_children_music)

        return tracks.join(filtered_tracks, c.TRACK_GROUP, how="left_anti")

    @staticmethod
    def apply_invalid_filters(category_filters: DataFrame,
                              stream_count: int,
                              streamers_count: int,
                              min_track_duration: int,
                              max_track_duration: int,
                              drop_holiday: bool,
                              drop_ambient: bool,
                              drop_children: bool):
        all_checks = category_filters.where((F.col(c.AVAILABLE) == False) |
                                            (F.col(c.NON_MUSIC) == 1) |
                                            (F.col(c.STREAM_COUNT) < stream_count) |
                                            (F.col(c.STREAMERS_COUNT) < streamers_count) |
                                            ~F.col(c.DURATION).between(min_track_duration, max_track_duration)
                                            ).select(c.TRACK_GROUP)
        category_filters = apply_invalid_category_filters(dataframe=category_filters,
                                                          drop_holiday=drop_holiday,
                                                          drop_ambient=drop_ambient,
                                                          drop_children=drop_children,
                                                          key=c.TRACK_GROUP).select(c.TRACK_GROUP)
        return all_checks.union(category_filters)

    @staticmethod
    def apply_valid_filters(category_filters: DataFrame,
                            stream_count: int,
                            streamers_count: int,
                            min_track_duration: int,
                            max_track_duration: int,
                            drop_holiday: bool,
                            drop_ambient: bool,
                            drop_children: bool):
        df = category_filters.where((F.col(c.AVAILABLE) == True) &
                                    (F.col(c.NON_MUSIC) == 0) &
                                    (F.col(c.STREAM_COUNT) >= stream_count) &
                                    (F.col(c.STREAMERS_COUNT) >= streamers_count) &
                                    F.col(c.DURATION).between(min_track_duration, max_track_duration)
                                    )
        return apply_valid_category_filters(dataframe=df,
                                            drop_holiday=drop_holiday,
                                            drop_ambient=drop_ambient,
                                            drop_children=drop_children,
                                            key=c.TRACK_GROUP).select(c.TRACK_GROUP)


def apply_invalid_category_filters(dataframe: DataFrame,
                                   drop_holiday: bool,
                                   drop_ambient: bool,
                                   drop_children: bool,
                                   key: str):
    dropped_dataframe = dataframe
    if drop_children:
        dropped_dataframe = dropped_dataframe.where(F.col(c.CHILDREN) == 0)

    if drop_ambient:
        dropped_dataframe = dropped_dataframe.where(F.col(c.AMBIENT) == 0)

    if drop_holiday:
        dropped_dataframe = dropped_dataframe.where(F.col(c.HOLIDAY) == 0)

    return dataframe.join(dropped_dataframe.select(key), key, "left_anti")


def apply_valid_category_filters(dataframe: DataFrame,
                                 drop_holiday: bool,
                                 drop_ambient: bool,
                                 drop_children: bool,
                                 key: str):
    if drop_children:
        dataframe = dataframe.where(F.col(c.CHILDREN) == 0)

    if drop_ambient:
        dataframe = dataframe.where(F.col(c.AMBIENT) == 0)

    if drop_holiday:
        dataframe = dataframe.where(F.col(c.HOLIDAY) == 0)

    return dataframe.select(key)
