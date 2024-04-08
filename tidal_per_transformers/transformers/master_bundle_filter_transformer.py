from typing import Optional, List

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
from tidal_per_transformers.transformers.track_group_filter_transformer import apply_category_filters


class MasterBundleFilterTransformer(LoggableTransformer):
    """
    Transformer for removing albums based on either category,
    popularity (i.e., # streams and # streamers from the last 12 months) or a combination of the two.
    """

    def __init__(self,
                 album_filters: DataFrame,
                 remove_children_music: bool,
                 remove_ambient_music: bool,
                 remove_holiday_music: bool,
                 remove_variant_versions: bool,
                 min_album_streamers: int,
                 min_album_streams: int,
                 album_column: str = c.MASTER_BUNDLE_ID,
                 album_types: Optional[List[str]] = None):
        """
        :param album_filters:          DataFrame containing the content filter information
        :param remove_ambient_music:    flag for toggling ambient music on/off
        :param remove_holiday_music:    flag for toggling holiday music on/off
        :param remove_children_music:   flag for toggling children music on/off
        :param remove_variant_versions: flag for toggling variant versions (e.g., karaoke, covers) on/off
        :param min_album_streamers:     minimum number of streams for an album to be included
        :param min_album_streams:       minimum number of unique listeners for an album to be included
        :param album_column:            Column containing master bundle id
        """
        super().__init__()
        self.album_filters = album_filters
        self.remove_children_music = remove_children_music
        self.remove_ambient_music = remove_ambient_music
        self.remove_holiday_music = remove_holiday_music
        self.remove_variant_versions = remove_variant_versions
        self.min_album_streamers = min_album_streamers
        self.min_album_streams = min_album_streams
        self.album_column = album_column
        self.album_types = album_types or [c.ALBUM.upper()]

    def _transform(self, dataset):
        cleaned_albums = self.apply_filters(self.album_filters,
                                            stream_count=self.min_album_streams,
                                            streamers_count=self.min_album_streamers,
                                            album_types=self.album_types,
                                            drop_holiday=self.remove_holiday_music,
                                            drop_ambient=self.remove_ambient_music,
                                            drop_children=self.remove_children_music,
                                            drop_variant_versions=self.remove_variant_versions)

        return (dataset
                .join(cleaned_albums.withColumnRenamed(c.MASTER_BUNDLE_ID, self.album_column),
                      self.album_column))

    @staticmethod
    def apply_filters(category_filters: DataFrame,
                      stream_count: int,
                      streamers_count: int,
                      album_types: List[str],
                      drop_holiday: bool,
                      drop_ambient: bool,
                      drop_children: bool,
                      drop_variant_versions: bool) -> DataFrame:
        """Applies filters given in a data frame

        :param category_filters:        dataframe containing filters
        :param stream_count:            min stream count
        :param streamers_count:         min streamers count
        :param album_types:             list of album types
        :param drop_holiday:            flag to drop holiday
        :param drop_ambient:            flag to drop ambient music
        :param drop_children:           flag to drop children
        :param drop_variant_versions:   flag to drop variant versions
        :return: cleaned dataframe
        """
        albums = apply_category_filters(dataframe=category_filters
                                        .where(F.col(c.AVAILABLE))
                                        .where(F.col(c.ALBUM_TYPE).isin(album_types))
                                        .where(F.col(c.NON_MUSIC) == 0)
                                        .where(F.col(c.STREAM_COUNT) >= stream_count)
                                        .where(F.col(c.STREAMERS_COUNT) >= streamers_count),
                                        drop_holiday=drop_holiday,
                                        drop_ambient=drop_ambient,
                                        drop_children=drop_children)

        if drop_variant_versions:
            albums = albums.where(F.col(c.VARIANT) == 0)

        return albums.select(c.MASTER_BUNDLE_ID)
