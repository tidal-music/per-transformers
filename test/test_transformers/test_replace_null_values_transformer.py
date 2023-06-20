import pyspark.sql.functions as F
import transformers.utils.constants as c
from transformers.replace_null_values_transformer import ReplaceNullValuesTransformer
from pyspark_test import PySparkTest


class ReplaceNullValuesTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, c.NAME, "a"),
            (2, None, "a"),
            (3, c.NAME, None),
        ], [c.ARTIFACT_ID, c.TYPE, c.VALUE])

        output = (ReplaceNullValuesTransformer({c.TYPE: "unknown", c.VALUE: "default"})
                  .transform(dataset))

        self.assertEqual("unknown", output.where(F.col(c.ARTIFACT_ID) == 2).select(c.TYPE).collect()[0][0])
        self.assertEqual("default", output.where(F.col(c.ARTIFACT_ID) == 3).select(c.VALUE).collect()[0][0])
