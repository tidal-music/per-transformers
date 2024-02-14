import tidal_per_transformers.transformers.utils.constants as c
from pyspark_test import PySparkTest
from tidal_per_transformers.transformers.track_group_to_product_id_mapping_transformer import \
    TrackGroupToProductIdMappingByUserCountryTransformer


class TrackGroupToProductIdMappingByUserCountryTransformerTest(PySparkTest):

    def test_transform(self):
        track_metadata = self.spark.createDataFrame([
            (3, '1', ['US', 'AE']),
            (2, '1', ['US', 'NO']),
            (1, '1', ['AE'])
        ], [c.ID, c.TRACK_GROUP, c.AVAILABLE_COUNTRY_CODES])

        user_countries = self.spark.createDataFrame([
            (1, 'US')
        ], [c.USER_ID, c.COUNTRY_CODE])

        df = self.spark.createDataFrame([
            (1, '1')
        ], [c.USER_ID, c.TRACK_GROUP])

        res = TrackGroupToProductIdMappingByUserCountryTransformer(
            track_metadata=track_metadata,
            user_countries=user_countries).transform(df).collect()

        self.assertEqual(1, len(res))
        self.assertEqual(1, res[0][c.USER_ID])
        self.assertEqual('1', res[0][c.TRACK_GROUP])
        self.assertEqual(2, res[0][c.PRODUCT_ID])
