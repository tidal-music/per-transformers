from transformers.select_transformer import SelectTransformer
from pyspark_test import PySparkTest

import transformers.utils.constants as c
import pyspark.sql.functions as F


class SelectTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, [10, 11, 12], 100),
            (1, [10, 11, 12], 100),
        ], [c.PLAYLIST_ID, c.TRACKS, c.USER_ID])

        res = SelectTransformer([F.col(c.PLAYLIST_ID).alias(c.ID), c.TRACKS]).transform(dataset)

        self.assertEqual({c.ID, c.TRACKS}, set(res.columns))
