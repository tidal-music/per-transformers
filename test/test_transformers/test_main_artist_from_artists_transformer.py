# import pyspark.sql.functions as F
#
# import utils.constants as c
# from transformers import MainArtistFromArtistsTransformer
# from pyspark_commons_test.datasets import get_missing_main_artists
# from pyspark_test import PySparkTest
#
#
# class MainArtistFromArtistsTransformerTest(PySparkTest):
#
#     def test_transform(self):
#         dataset = get_missing_main_artists(self.sc)
#
#         res = MainArtistFromArtistsTransformer().transform(dataset)
#
#         self.assertEqual(set(res.columns), set(dataset.columns + [c.ARTIST_ID]))
#         self.assertEqual(res.count(), res.where(~F.col(c.ARTIST_ID).isNull()).count())
