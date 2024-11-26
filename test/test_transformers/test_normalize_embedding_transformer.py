from math import sqrt

from pyspark_test import PySparkTest
from tidal_per_transformers.transformers import NormalizeEmbeddingTransformer


class NormalizeEmbeddingTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            ([1, 0],),
            ([0, -1],),
            ([-1, -1],),
            ], ["embedding"])

        res = NormalizeEmbeddingTransformer(col="embedding", order=None).transform(dataset).collect()

        self.assertEqual([1, 0], res[0]["embedding"])
        self.assertEqual([0, -1], res[1]["embedding"])
        self.assertAlmostEqual(-sqrt(2) / 2, res[2]["embedding"][0], places=4)
        self.assertAlmostEqual(-sqrt(2) / 2, res[2]["embedding"][1], places=4)