import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.dataframe import DataFrame

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers import ArtistFilterTransformer


class ArtistsStructFilterTransformer(ArtistFilterTransformer):
    """
    :param artist_filters:          DataFrame containing the content filter information
    :param remove_ambient_music:    flag for toggling ambient music on/off
    :param remove_holiday_music:    flag for toggling holiday music on/off
    :param remove_children_music:   flag for toggling children music on/off
    :param min_artist_streams:      minimum number of streams for an artist to be included
    :param min_artist_streamers:    minimum number of unique listeners for an artist to be included
    :param artist_column:           Column containing artists
    """

    def __init__(self,
                 artist_filters: DataFrame,
                 remove_children_music: bool,
                 remove_ambient_music: bool,
                 remove_holiday_music: bool,
                 min_artist_streamers: int = 500,
                 min_artist_streams: int = 2000,
                 artist_column: str = c.ARTISTS
                 ):
        super().__init__(artist_filters,
                         remove_children_music,
                         remove_ambient_music,
                         remove_holiday_music,
                         min_artist_streamers,
                         min_artist_streams)
        self.artist_filters = artist_filters
        self.remove_children_music = remove_children_music
        self.remove_ambient_music = remove_ambient_music
        self.remove_holiday_music = remove_holiday_music
        self.min_artist_streamers = min_artist_streamers
        self.min_artist_streams = min_artist_streams
        self.artist_column = artist_column

    def _transform(self, dataset):
        cleaned_artists = self.apply_filters(self.artist_filters,
                                             self.min_artist_streams,
                                             self.min_artist_streamers,
                                             self.remove_holiday_music,
                                             self.remove_ambient_music,
                                             self.remove_children_music)
        artists_set = set(cleaned_artists.toPandas()[c.ARTIST_ID].tolist())
        artists_set_bc = dataset.sql_ctx.sparkSession.sparkContext.broadcast(artists_set)

        @F.udf(returnType=T.ArrayType(T.IntegerType()))
        def filter_artists(main_artists):
            return [artist[c.ID] for artist in main_artists if artist[c.ID] in artists_set_bc.value]

        return (dataset
                .withColumn(c.MATCHES, filter_artists(self.artist_column))
                .where(F.size(c.MATCHES) > 0)
                .drop(c.MATCHES))
