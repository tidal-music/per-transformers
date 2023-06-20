from abc import abstractmethod

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
from tidal_per_transformers.transformers.track_group_filter_transformer import apply_category_filters


class ArtistFilterTransformer(LoggableTransformer):
    """
    Transformer for removing artists based on either category, popularity or a combination of the two. E.g. if you
    want the artist recommender feature to not recommend children music you can choose to filter all artists tagged
    with the children music tag. The popularity features are based on streaming data from the last 12 months
    """

    def __init__(self,
                 artist_filters: DataFrame,
                 remove_children_music: bool,
                 remove_ambient_music: bool,
                 remove_holiday_music: bool,
                 min_artist_streamers: int,
                 min_artist_streams: int):
        """
        :param artist_filters:          DataFrame containing the content filter information
        :param remove_ambient_music:    flag for toggling ambient music on/off
        :param remove_holiday_music:    flag for toggling holiday music on/off
        :param remove_children_music:   flag for toggling children music on/off
        :param min_artist_streams:      minimum number of streams for an artist to be included
        :param min_artist_streamers:    minimum number of unique listeners for an artist to be included
        """
        super(ArtistFilterTransformer, self).__init__()
        self.artist_filters = artist_filters
        self.remove_children_music = remove_children_music
        self.remove_ambient_music = remove_ambient_music
        self.remove_holiday_music = remove_holiday_music
        self.min_artist_streamers = min_artist_streamers
        self.min_artist_streams = min_artist_streams

    @abstractmethod
    def _transform(self, dataset):
        ...

    @staticmethod
    def apply_filters(category_filters: DataFrame,
                      stream_count: int,
                      streamers_count: int,
                      drop_holiday: bool,
                      drop_ambient: bool,
                      drop_children: bool) -> DataFrame:
        """Applies filters given in a data frame

        :param category_filters:    dataframe containing filters
        :param stream_count:        min stream count
        :param streamers_count:     min streamers count
        :param drop_holiday:        flag to drop holiday
        :param drop_ambient:        flag to drop ambient music
        :param drop_children:       flag to drop children
        :return: cleaned dataframe
        """
        return apply_category_filters(dataframe=category_filters
                                      .where(F.col(c.AVAILABLE))
                                      .where(F.col(c.NON_MUSIC) == 0)
                                      .where(F.col(c.STREAM_COUNT) >= stream_count)
                                      .where(F.col(c.STREAMERS_COUNT) >= streamers_count),
                                      drop_holiday=drop_holiday,
                                      drop_ambient=drop_ambient,
                                      drop_children=drop_children).select(c.ARTIST_ID)
