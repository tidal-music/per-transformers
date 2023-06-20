import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.distinct_transformer import DistinctTransformer
from pyspark_test import PySparkTest


class DistinctTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, c.NAME, "a"),
            (1, c.NAME, "a"),
            (1, c.NAME, "c"),
        ], [c.ARTIFACT_ID, c.TYPE, c.VALUE])

        output = (DistinctTransformer()
                  .transform(dataset))

        self.assertEqual(2, output.count())
