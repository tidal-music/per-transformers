from pyspark import Row

from transformers.order_by_transformer import OrderByTransformer
from pyspark_test import PySparkTest

import utils.constants as c
import pyspark.sql.functions as F


class OrderByTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, 2),
            (2, 3),
            (2, 4),
            (2, 4),
            (3, 1)
        ], [c.ID, c.PLAYLIST_ID])

        res = OrderByTransformer(F.desc(c.PLAYLIST_ID)).transform(dataset)

        self.assertEqual([Row(id=2, playlistId=4),
                          Row(id=2, playlistId=4),
                          Row(id=2, playlistId=3),
                          Row(id=1, playlistId=2),
                          Row(id=3, playlistId=1)],
                         res.collect())
