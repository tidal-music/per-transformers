from pyspark.sql.types import StructType, StructField, LongType, StringType, Row
from transformers.with_column_transfomer import WithColumnTransformer
from pyspark_test import PySparkTest

import utils.constants as c
import pyspark.sql.functions as F


class WithColumnTransformerTest(PySparkTest):

    def test_transform(self):
        df = self.sc.createDataFrame([
            (1, "a"),
            (2, "b"),
        ], [c.ID, c.NAME])

        output = WithColumnTransformer(c.TITLE, F.lit('c')).transform(df)
        self.assertEqual([Row(id=1, name='a', title='c'),
                          Row(id=2, name='b', title='c')], output.collect())
        schema = StructType([
            StructField(c.ID, LongType()),
            StructField(c.NAME, StringType()),
            StructField(c.TITLE, StringType(), False)
        ])

        self.assertEqual(schema, output.schema)

