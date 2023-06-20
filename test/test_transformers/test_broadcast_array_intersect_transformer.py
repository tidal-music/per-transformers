import pyspark.sql.functions as F

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.broadcast_array_intersect_transformer import BroadcastArrayIntersectTransformer
from pyspark_test import PySparkTest


class BroadcastArrayIntersectTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, [1, 2, 3, 4, 5]),
            (2, [7, 6, 3, 2])
        ], [c.ID, c.ITEMS])

        items_to_keep = self.spark.createDataFrame([
            (1, ),
            (2, ),
            (3, ),
        ], [c.ITEM])

        res = (BroadcastArrayIntersectTransformer(c.ITEMS,
                                                  items_to_keep
                                                  .agg(F.collect_set(c.ITEM).alias(c.ITEMS_TO_KEEP)))
               .transform(dataset)
               .sort(c.ID)
               .collect())

        self.assertEqual(2, len(res))
        self.assertTrue([1, 2, 3], [int(v) for v in res[0][c.ITEMS]])
        self.assertTrue([3, 2], [int(v) for v in res[1][c.ITEMS]])
