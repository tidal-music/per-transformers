from pyspark.sql.types import StructType, StructField, LongType, StringType, Row

from transformers.with_column_renamed_transfomer import WithColumnRenamedTransformer
from pyspark_test import PySparkTest

import utils.constants as c


class WithColumnRenamedTransformerTest(PySparkTest):

    def test_transform(self):
        df = self.sc.createDataFrame([
            (1, "a"),
            (2, "b"),
        ], [c.ID, c.NAME])

        output = WithColumnRenamedTransformer(c.NAME, c.TITLE).transform(df)
        self.assertEqual([Row(id=1, title='a'),
                          Row(id=2, title='b')], output.collect())
        schema = StructType([
            StructField(c.ID, LongType()),
            StructField(c.TITLE, StringType())
        ])

        self.assertEqual(schema, output.schema)

