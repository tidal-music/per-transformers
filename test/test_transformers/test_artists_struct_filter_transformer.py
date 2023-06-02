from transformers.artists_struct_filter_transformer import ArtistsStructFilterTransformer
from pyspark_test import PySparkTest
import utils.constants as c
from utils.schemas import ARTISTS_SCHEMA
import pyspark.sql.types as T


class TestArtistsStructFilterTransformer(PySparkTest):

    def setUp(self):
        self.artist_category_filters = self.spark.createDataFrame([
            (65, "The Clash", True, 100_000, 30_000, 0, 0, 0, 0),
            (222, "The Greatful Dead", True, 130_000, 48_000, 0, 0, 0, 0),
            (23406735, "Kassa Young", True, 200, 21, 0, 0, 0, 0),
            (23466271, "Hel1k", True, 3, 1, 0, 0, 0, 0),
            (4031806, "Rain Sounds", True, 80_000, 22_000, 0, 1, 0, 0),
            (8977873, "Pinkfong", True, 220_000, 110_000, 1, 0, 0, 0),
            (7103647, "Xmas Classics", True, 20_000, 9_000, 0, 0, 1, 0),
            (3701475, "Katt Williams", True, 9_000, 6_000, 0, 0, 0, 1)],
            [c.ARTIST_ID,
             c.NAME,
             c.AVAILABLE,
             c.STREAM_COUNT,
             c.STREAMERS_COUNT,
             c.CHILDREN,
             c.AMBIENT,
             c.HOLIDAY,
             c.NON_MUSIC])

        self.artists = self.spark.createDataFrame([
            ("abc", [{"aliases": [], "picture": "", "id": 65, "name": "The Clash", "main": True}]),
            ("abd", [{"aliases": [], "picture": "", "id": 222, "name": "The Greatful Dead", "main": True}]),
            ("abe", [
                {"aliases": [], "picture": "", "id": 23406735, "name": "Kassa Young", "main": True},
                {"aliases": [], "picture": "", "id": 65, "name": "The Clash", "main": True}]),  # 1 valid artist
            ("abf", [{"aliases": [], "picture": "", "id": 23466271, "name": "Hel1k", "main": True}]),
            ("abg", [{"aliases": [], "picture": "", "id": 4031806, "name": "Rain Sounds", "main": True}]),
            ("abh", [{"aliases": [], "picture": "", "id": 8977873, "name": "Pinkfong", "main": True}]),
            ("abi", [{"aliases": [], "picture": "", "id": 7103647, "name": "Xmas Classics", "main": True}]),
            ("abj", [{"aliases": [], "picture": "", "id": 3701475, "name": "Katt Williams", "main": True}]),
        ], schema=T.StructType([T.StructField(c.TRACK_GROUP, T.StringType()),
                                T.StructField(c.ARTISTS, ARTISTS_SCHEMA)]))

    def test_transform_artists(self):
        res = ArtistsStructFilterTransformer(self.artist_category_filters,
                                             remove_children_music=True,
                                             remove_ambient_music=True,
                                             remove_holiday_music=True,
                                             min_artist_streams=5_000,
                                             min_artist_streamers=1_000,
                                             ).transform(self.artists)

        self.assertEqual(3, res.count())
        self.assertEqual(self.artists.columns, res.columns)

        res_with_children = ArtistsStructFilterTransformer(self.artist_category_filters,
                                                           remove_children_music=False,
                                                           remove_ambient_music=True,
                                                           remove_holiday_music=True,
                                                           min_artist_streams=5_000,
                                                           min_artist_streamers=1_000,
                                                           ).transform(self.artists)

        self.assertEqual(4, res_with_children.count())

        res_min_filters = ArtistsStructFilterTransformer(self.artist_category_filters,
                                                         remove_children_music=False,
                                                         remove_ambient_music=False,
                                                         remove_holiday_music=False,
                                                         min_artist_streams=0,
                                                         min_artist_streamers=0,
                                                         ).transform(self.artists)

        self.assertEqual(7, res_min_filters.count())
