import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.artist_filter_transformer import ArtistFilterTransformer
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
from tidal_per_transformers.transformers.track_group_filter_transformer import TrackGroupFilterTransformer
from tidal_per_transformers.transformers.utils.schemas import PLAYLIST_TRACKS_SCHEMA

MATCHES = "matches"


class SequenceContentFilterTransformer(LoggableTransformer):
    """
    Transformer for applying content filters to columns with lists of tracks. This can e.g. be applied to the
    playlist track table containing a list of all the tracks in a playlist.
    """
    def __init__(self,
                 track_filters: DataFrame,
                 artist_filters: DataFrame,
                 remove_children_music: bool,
                 remove_ambient_music: bool,
                 remove_holiday_music: bool,
                 min_track_streams: int = 500,
                 min_track_streamers: int = 200,
                 min_track_duration: int = 45,
                 max_track_duration: int = 1200,
                 min_artist_streamers: int = 1000,
                 min_artist_streams: int = 2000,
                 schema=PLAYLIST_TRACKS_SCHEMA):
        """
        :param artist_filters:          DataFrame containing the content filter information
        :param remove_ambient_music:    flag for toggling ambient music on/off
        :param remove_holiday_music:    flag for toggling holiday music on/off
        :param remove_children_music:   flag for toggling children music on/off
        :param min_track_streams:       minimum number of recorded streams
        :param min_track_streamers:     minimum number of unique listeners
        :param min_track_duration:      minimum allowed duration of a track
        :param max_track_duration:      maximum allowed duration of a track
        :param min_artist_streams:      minimum number of streams for an artist to be included
        :param min_artist_streamers:    minimum number of unique listeners for an artist to be included
        """
        super(SequenceContentFilterTransformer, self).__init__()
        self.artist_filters = artist_filters
        self.track_filters = track_filters
        self.remove_children_music = remove_children_music
        self.remove_ambient_music = remove_ambient_music
        self.remove_holiday_music = remove_holiday_music
        self.min_artist_streamers = min_artist_streamers
        self.min_artist_streams = min_artist_streams
        self.min_track_streams = min_track_streams
        self.min_track_streamers = min_track_streamers
        self.min_track_duration = min_track_duration
        self.max_track_duration = max_track_duration
        self.schema = schema

    def _transform(self, dataset):
        artists_set = set(self.get_cleaned_artists().toPandas()[c.ARTIST_ID].tolist())
        artists_bc = dataset.sql_ctx.sparkSession.sparkContext.broadcast(artists_set)

        tracks_set = set(self.get_cleaned_track_groups().toPandas()[c.TRACK_GROUP].tolist())
        tracks_bc = dataset.sql_ctx.sparkSession.sparkContext.broadcast(tracks_set)

        @F.udf(returnType=self.schema)
        def filter_tracks(tracks):
            return [t for t in tracks if t[c.ARTIST_ID] in artists_bc.value and t[c.TRACK_GROUP] in tracks_bc.value]

        return (dataset
                .withColumn(c.TRACKS, filter_tracks(c.TRACKS))
                .where(F.size(c.TRACKS) > 0))  # Drop empty sequences

    def get_cleaned_artists(self):
        return ArtistFilterTransformer.apply_filters(self.artist_filters,
                                                     self.min_artist_streams,
                                                     self.min_artist_streamers,
                                                     self.remove_holiday_music,
                                                     self.remove_ambient_music,
                                                     self.remove_children_music).select(c.ARTIST_ID)

    def get_cleaned_track_groups(self):
        return TrackGroupFilterTransformer.apply_filters(self.track_filters,
                                                         self.min_track_streams,
                                                         self.min_track_streamers,
                                                         self.min_track_duration,
                                                         self.max_track_duration,
                                                         self.remove_holiday_music,
                                                         self.remove_ambient_music,
                                                         self.remove_children_music).select(c.TRACK_GROUP)
