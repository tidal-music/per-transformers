import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.pivot_transformer import PivotTransformer
from pyspark_test import PySparkTest


class PivotTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, c.NAME, "a"),
            (1, c.GENRE, "b"),
            (1, c.GENRE, "c"),
        ], [c.ARTIFACT_ID, c.TYPE, c.VALUE])

        output = (PivotTransformer(c.ARTIFACT_ID, c.TYPE, F.collect_list(c.VALUE))
                  .transform(dataset))

        self.assertEqual({c.ARTIFACT_ID, c.NAME, c.GENRE}, set(output.columns))
        self.assertEqual({"b", "c"}, set(output.select(c.GENRE).collect()[0][0]))
