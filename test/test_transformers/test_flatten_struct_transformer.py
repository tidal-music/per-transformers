from pyspark.sql.types import StructType, StructField, LongType, StringType, Row, ArrayType

from transformers.flatten_struct_transformer import FlattenStructTransformer
from pyspark_test import PySparkTest
import utils.constants as c


class FlattenStructTransformerTest(PySparkTest):

    def test_transform(self):

        artists = self.spark.createDataFrame([
            (1, [Row(name='John', age=30), Row(name='Mary', age=50)]),
            (2, [Row(name='Mike', age=20), Row(name='Julie', age=60)]),
        ], [c.ID, c.ARTISTS])

        struct_fields = {c.ARTIST_NAME: f"{c.ARTISTS}.{c.NAME}",
                         c.ARTIST_AGE: f"{c.ARTISTS}.{c.AGE}"}

        output = FlattenStructTransformer(struct_fields).transform(artists)
        self.assertEqual({c.ARTIST_NAME: ['John', 'Mary'],
                          c.ARTIST_AGE: [30, 50]},
                         output.select(c.ARTIST_NAME, c.ARTIST_AGE).collect()[0].asDict())
        schema = StructType([
            StructField(c.ID, LongType()),
            StructField(c.ARTISTS, ArrayType(StructType([
                StructField(c.NAME, StringType()),
                StructField(c.AGE, LongType())]
            ))),
            StructField(c.ARTIST_NAME, ArrayType(StringType())),
            StructField(c.ARTIST_AGE, ArrayType(LongType()))
        ])

        self.assertEqual(schema, output.schema)

