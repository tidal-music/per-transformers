# import pyspark.sql.functions as F
# import pyspark.sql.types as T
#
# from transformers.filter_infrequent_seq_transformer import FilterInfrequentSeqItemsTransformer
# from pyspark_test import PySparkTest
# from utils.schemas import PLAYLIST_TRACKS_SCHEMA
# from pyspark_commons_test.datasets import get_playlist_metadata
# import utils.constants as c
#
#
# class FilterInfrequentSeqItemsTransformerTest(PySparkTest):
#
#     def test_transform_struct(self):
#         dataset = get_playlist_metadata(self.sc)
#
#         res = FilterInfrequentSeqItemsTransformer(5,
#                                                   c.TRACKS,
#                                                   c.TRACK_GROUP,
#                                                   c.PLAYLIST_ID,
#                                                   sequence_schema=PLAYLIST_TRACKS_SCHEMA).transform(dataset)
#
#         vocab_size_before = dataset.select(F.explode(f"{c.TRACKS}.{c.TRACK_GROUP}")).distinct().count()
#         vocab_size_after = res.select(F.explode(f"{c.TRACKS}.{c.TRACK_GROUP}")).distinct().count()
#
#         self.assertTrue(vocab_size_before > vocab_size_after)
#         self.assertTrue(vocab_size_after > 800)
#
#     def test_transform_list(self):
#         dataset = self.sc.createDataFrame([
#             (1, [10, 11, 11, 12, 13]),
#             (2, [12, 13, 14, 15, 16]),
#             (3, [10, 12, 14, 14, 14]),
#             (4, [10, 11, 13]),
#             (5, [15, 17, 18, 19])
#         ], [c.PLAYLIST_ID, c.TRACKS])
#
#         res = (FilterInfrequentSeqItemsTransformer(2,
#                                                    c.TRACKS,
#                                                    None,
#                                                    c.PLAYLIST_ID,
#                                                    T.ArrayType(T.IntegerType())).transform(dataset))
#
#         vocab_size_after = res.select(F.explode(c.TRACKS)).distinct().count()
#
#         self.assertEqual(6, vocab_size_after)
#         self.assertEqual([10, 11, 11, 12, 13], res.collect()[0][c.TRACKS])
#         self.assertEqual([15], res.collect()[-1][c.TRACKS])
