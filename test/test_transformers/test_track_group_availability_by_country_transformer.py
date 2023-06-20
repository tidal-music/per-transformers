import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.track_group_availability_by_country_transformer import \
    TrackGroupAvailabilityByCountryTransformer
from pyspark_test import PySparkTest


class TrackGroupAvailabilityByCountryTransformerTest(PySparkTest):

    def test_transformer(self):
        track_group_metadata = self.spark.createDataFrame([
            ("1", ["NO", "US", "UK"]),
            ("2", ["NO", "US"]),
        ], [c.TRACK_GROUP, c.AVAILABLE_COUNTRY_CODES])

        dataset = self.spark.createDataFrame([
            ("1", "NO", 1001),
            ("1", "UK", 1002),
            ("1", "CN", 1002),
            ("3", "NO", 1005),
        ], [c.TRACK_GROUP, c.COUNTRY_CODE, c.PRODUCT_ID])

        # filter out track for unavailable countries
        res = TrackGroupAvailabilityByCountryTransformer(track_group_metadata)
        res = res.transform(dataset)
        self.assertEqual(dataset.schema, res.schema)
        self.assertEqual(2, res.count())
        self.assertEqual(1, res.where(F.col(c.PRODUCT_ID) == 1002).count())
