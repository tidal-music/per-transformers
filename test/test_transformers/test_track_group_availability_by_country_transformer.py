# import pyspark.sql.functions as F
# import utils.constants as c
# from datasets.models import get_track_group_availability
# from transformers.track_group_availability_by_country_transformer import \
#     TrackGroupAvailabilityByCountryTransformer
# from pyspark_test import PySparkTest
#
#
# class TrackGroupAvailabilityByCountryTransformerTest(PySparkTest):
#
#     def test_transformer(self):
#         track_group_metadata = self.sc.createDataFrame([
#             ("1", ["NO", "US", "UK"], 1002),
#             ("2", ["NO", "US"], 1003),
#         ], [c.TRACK_GROUP, c.AVAILABLE_COUNTRY_CODES, c.PRODUCT_ID])
#
#         dataset = self.sc.createDataFrame([
#             ("1", "NO", 1001),
#             ("1", "UK", 1002),
#             ("1", "CN", 1002),
#             ("3", "NO", 1005),
#         ], [c.TRACK_GROUP, c.COUNTRY_CODE, c.PRODUCT_ID])
#
#         # filter out track for unavailable countries
#         res = TrackGroupAvailabilityByCountryTransformer(get_track_group_availability(track_group_metadata))
#         res = res.transform(dataset)
#         self.assertEqual(dataset.schema, res.schema)
#         self.assertEqual(2, res.count())
#         self.assertEqual(1, res.where(F.col(c.PRODUCT_ID) == 1002).count())
