from transformers.single_artist_filter_transformer import SingleArtistFilterTransformer
from pyspark_test import PySparkTest
import transformers.utils.constants as c


class TestSingleArtistFilterTransformer(PySparkTest):

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

    def test_transform_artist_id(self):
        artists = self.spark.createDataFrame([
            (65,),
            (222,),
            (23406735,),
            (23466271,),
            (4031806,),
            (8977873,),
            (7103647,),
            (3701475,)
        ], [c.ARTIST_ID])

        res = SingleArtistFilterTransformer(self.artist_category_filters,
                                            remove_children_music=True,
                                            remove_ambient_music=True,
                                            remove_holiday_music=True,
                                            min_artist_streams=5_000,
                                            min_artist_streamers=1_000,
                                            ).transform(artists)

        self.assertEqual(2, res.count())
        self.assertEqual(artists.columns, res.columns)

        res_with_children = SingleArtistFilterTransformer(self.artist_category_filters,
                                                          remove_children_music=False,
                                                          remove_ambient_music=True,
                                                          remove_holiday_music=True,
                                                          min_artist_streams=5_000,
                                                          min_artist_streamers=1_000,
                                                          ).transform(artists)

        self.assertEqual(3, res_with_children.count())

        res_min_filters = SingleArtistFilterTransformer(self.artist_category_filters,
                                                        remove_children_music=False,
                                                        remove_ambient_music=False,
                                                        remove_holiday_music=False,
                                                        min_artist_streams=0,
                                                        min_artist_streamers=0,
                                                        ).transform(artists)

        self.assertEqual(7, res_min_filters.count())
