import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.sequence_content_filter_transformer import SequenceContentFilterTransformer
from pyspark_test import PySparkTest
from pyspark import Row
import pyspark.sql.types as T
import pyspark.sql.functions as F


class SequenceContentFilterTransformerTest(PySparkTest):

    def test_transform(self):
        playlists = self.spark.createDataFrame([Row(playlistId=1,
                                                    numberOfTracks=1,
                                                    genre='alternative',
                                                    userId=1,
                                                    playlistName='The x',
                                                    description='', deleted=False,
                                                    countryCode='US',
                                                    tracks=[Row(index=0,
                                                                productId=1,
                                                                title='S',
                                                                trackGroup='96',
                                                                albumId=1,
                                                                artistId=1,
                                                                duration=183.0,
                                                                masterBundle=6,
                                                                audioQuality='LOSSLESS',
                                                                genre='alternative')]
                                                    )])
        playlists = (playlists
                     .withColumn(c.PLAYLIST_ID, F.col(c.PLAYLIST_ID).cast(T.IntegerType()))
                     .withColumn("numberOfTracks", F.col("numberOfTracks").cast(T.IntegerType()))
                     .withColumn(c.TRACKS,
                                 F.transform(
                                     F.col(c.TRACKS),
                                     lambda x: x
                                     .withField("index", x["index"].cast("int"))
                                     .withField("productId", x["productId"].cast("int"))
                                     .withField("title", x["title"])
                                     .withField("trackGroup", x["trackGroup"])
                                     .withField("albumId", x["albumId"].cast("int"))
                                     .withField("artistId", x["artistId"].cast("int"))
                                     .withField("duration", x["duration"])
                                     .withField("masterBundle", x["masterBundle"].cast("int"))
                                     .withField("audioQuality", x["audioQuality"])
                                     .withField("genre", x["genre"]))
                                 )
                     )
        track_category_filters = self.get_track_group_content_filter_table()
        artist_category_filters = self.get_artist_content_filter_table()

        res = SequenceContentFilterTransformer(track_category_filters,
                                               artist_category_filters,
                                               remove_children_music=True,
                                               remove_ambient_music=True,
                                               remove_holiday_music=True,
                                               min_track_streams=500,
                                               min_track_streamers=200,
                                               min_track_duration=45,
                                               max_track_duration=1200,
                                               min_artist_streams=5_000,
                                               min_artist_streamers=1_000,
                                               ).transform(playlists)
        res.collect()

        self.assertEqual(1, res.count())

    def get_track_group_content_filter_table(self):
        return self.spark.createDataFrame([
            ("95476336", True, 300, 10_000, 5_000, 0, 0, 0, 0),
            ("46882336", True, 300, 100, 20, 0, 0, 0, 0),
            ("33176280", True, 900, 10_000, 5_000, 0, 0, 0, 0),
            ("34963569", True, 600, 10_000, 5_000, 0, 0, 0, 0),
            ("9166747", True, 300, 10_000, 5_000, 0, 0, 0, 0),
            ("10251579", True, 300, 10_000, 5_000, 0, 0, 0, 0),
            ("81728152", True, 300, 10_000, 5_000, 1, 0, 0, 0),
            ("94401208", True, 300, 10_000, 5_000, 0, 0, 0, 1),
        ], [c.TRACK_GROUP,
            c.AVAILABLE,
            c.DURATION,
            c.STREAM_COUNT,
            c.STREAMERS_COUNT,
            c.CHILDREN,
            c.HOLIDAY,
            c.AMBIENT,
            c.NON_MUSIC])

    def get_artist_content_filter_table(self):
        return self.spark.createDataFrame([
            (27446, True, 100_000, 30_000, 0, 0, 0, 0),
            (13591, True, 130_000, 48_000, 0, 0, 0, 0),
            (3565368, True, 200, 21, 0, 0, 0, 0),
            (3565245, True, 90_000, 11_000, 0, 0, 0, 0),
            (28481, True, 80_000, 22_000, 0, 0, 0, 0),
            (3604413, True, 220_000, 110_000, 0, 0, 0, 0),
            (7103647, True, 20_000, 9_000, 1, 0, 0, 0),
            (13596, True, 9_000, 6_000, 0, 0, 0, 1),
        ], [c.ARTIST_ID, c.AVAILABLE, c.STREAM_COUNT, c.STREAMERS_COUNT, c.CHILDREN, c.AMBIENT, c.HOLIDAY, c.NON_MUSIC])
