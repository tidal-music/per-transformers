from pyspark.sql.types import StructType, StructField, LongType, StringType, Row, ArrayType

from transformers.participating_artist_transformer import ParticipatingArtistsTransformer
from pyspark_test import PySparkTest
import utils.constants as c


class ParticipatingArtistsTransformerTest(PySparkTest):

    def test_transform(self):

        artists = self.sc.createDataFrame([
            (1, [Row(name='John', age=30, main='true'), Row(name='Mary', age=50, main='false')]),
            (2, [Row(name='Mike', age=20, main='true'), Row(name='Julie', age=60, main='true')]),
        ], [c.ID, c.ARTISTS])

        output = ParticipatingArtistsTransformer().transform(artists)
        self.assertEqual({c.MAIN_ARTISTS: [Row(name='John', age=30, main='true')],
                          c.FEATURING_ARTISTS: [Row(name='Mary', age=50, main='false')]},
                         output.select(c.MAIN_ARTISTS, c.FEATURING_ARTISTS).collect()[0].asDict())
        self.assertEqual({c.MAIN_ARTISTS:  [Row(name='Mike', age=20, main='true'),
                                            Row(name='Julie', age=60, main='true')],
                          c.FEATURING_ARTISTS: []},
                         output.select(c.MAIN_ARTISTS, c.FEATURING_ARTISTS).collect()[1].asDict())
        schema = StructType([
            StructField(c.ID, LongType()),
            StructField(c.ARTISTS, ArrayType(StructType([
                StructField(c.NAME, StringType()),
                StructField(c.AGE, LongType()),
                StructField(c.MAIN, StringType())]
            ))),
            StructField(c.MAIN_ARTISTS, ArrayType(StructType([
                StructField(c.NAME, StringType()),
                StructField(c.AGE, LongType()),
                StructField(c.MAIN, StringType())]
            ))),
            StructField(c.FEATURING_ARTISTS, ArrayType(StructType([
                StructField(c.NAME, StringType()),
                StructField(c.AGE, LongType()),
                StructField(c.MAIN, StringType())]
            )))
        ])

        self.assertEqual(schema, output.schema)

