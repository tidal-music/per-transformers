from pyspark.sql.types import StructType, StructField, LongType, StringType

from transformers.join_transformer import JoinTransformer
from pyspark_test import PySparkTest
import transformers.utils.constants as c


class JoinTransformerTest(PySparkTest):

    def test_transform(self):
        right = self.spark.createDataFrame([
            (1, "a"),
            (2, "b"),
        ], [c.ID, c.NAME])
        left = self.spark.createDataFrame([
            (1, "y"),
            (2, "z"),
        ], [c.ID, c.TITLE])

        output = JoinTransformer(right, c.ID).transform(left)
        self.assertEqual({'id': 1, 'title': 'y', 'name': 'a'}, output.collect()[0].asDict())
        schema = StructType([
            StructField(c.ID, LongType()),
            StructField(c.TITLE, StringType()),
            StructField(c.NAME, StringType())
        ])

        self.assertEqual(schema, output.schema)

