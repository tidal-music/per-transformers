import pyspark.sql.functions as F
import utils.constants as c
from transformers.pivot_transformer import PivotTransformer
from pyspark_test import PySparkTest


class PivotTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.sc.createDataFrame([
            (1, c.NAME, "a"),
            (1, c.GENRE, "b"),
            (1, c.GENRE, "c"),
        ], [c.ARTIFACT_ID, c.TYPE, c.VALUE])

        output = (PivotTransformer(c.ARTIFACT_ID, c.TYPE, F.collect_list(c.VALUE))
                  .transform(dataset))

        self.assertEqual({c.ARTIFACT_ID, c.NAME, c.GENRE}, set(output.columns))
        self.assertEqual({"b", "c"}, set(output.select(c.GENRE).collect()[0][0]))
