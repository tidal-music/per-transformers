import tidal_per_transformers.transformers.utils.constants as c
from pyspark_test import PySparkTest

from tidal_per_transformers.transformers.artifact_id_mapping_by_user_country_transformer import \
    ArtifactIdMappingByUserCountryTransformer


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

        res = ArtifactIdMappingByUserCountryTransformer(
            item_metadata=track_metadata,
            user_countries=user_countries).transform(df).collect()

        self.assertEqual(1, len(res))
        self.assertEqual(1, res[0][c.USER_ID])
        self.assertEqual('1', res[0][c.TRACK_GROUP])
        self.assertEqual(2, res[0][c.PRODUCT_ID])

    def test_map_master_album_id(self):
        album_metadata = self.spark.createDataFrame([
            (3, '1', ['US', 'AE']),
            (2, '1', ['US', 'NO']),
            (1, '1', ['AE'])
        ], [c.ID, c.MASTER_BUNDLE_ID, c.AVAILABLE_COUNTRY_CODES])

        user_countries = self.spark.createDataFrame([
            (1, 'US')
        ], [c.USER_ID, c.COUNTRY_CODE])

        df = self.spark.createDataFrame([
            (1, '1')
        ], [c.USER_ID, c.MASTER_BUNDLE_ID])

        res = ArtifactIdMappingByUserCountryTransformer(
            item_metadata=album_metadata,
            user_countries=user_countries,
            item_id_col=c.MASTER_BUNDLE_ID,
            item_instance_id_col=c.ALBUM_ID
        ).transform(df).collect()

        self.assertEqual(1, len(res))
        self.assertEqual(1, res[0][c.USER_ID])
        self.assertEqual('1', res[0][c.MASTER_BUNDLE_ID])
        self.assertEqual(2, res[0][c.ALBUM_ID])
