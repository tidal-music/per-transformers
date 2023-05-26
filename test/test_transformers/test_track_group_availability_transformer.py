from mock import patch

import utils.constants as c
from transformers import TrackGroupAvailabilityTransformer
from transformers.track_group_availability_transformer import get_available_track_groups
from pyspark_test import PySparkTest


class TrackGroupAvailabilityTransformerTest(PySparkTest):

    def test_transformer(self):
        track_metadata = self.sc.createDataFrame([
            ("1", ["NO", "US", "FR"], 1001),
            ("1", ["NO", "US", "UK"], 1002),
            ("2", ["NO", "US"], 1003),
        ], [c.TRACK_GROUP, c.AVAILABLE_COUNTRY_CODES, c.PRODUCT_ID])

        dataset = self.sc.createDataFrame([
            ("1", "NO", 1001),
            ("1", "UK", 1002),
            ("1", "CN", 1002),
            ("3", "NO", 1005),
        ], [c.TRACK_GROUP, c.COUNTRY_CODE, c.PRODUCT_ID])

        # filter out track groups that are unavailable for any countries
        res = TrackGroupAvailabilityTransformer(get_available_track_groups(track_metadata)).transform(dataset)
        self.assertEqual(dataset.schema, res.schema)
        self.assertEqual(3, res.count())
