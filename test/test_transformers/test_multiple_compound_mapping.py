from transformers.multiple_compound_mapping import MultipleCompoundTransformer
from pyspark_test import PySparkTest
import pyspark.sql.functions as F
import utils.constants as c


class MultipleCompoundTransformerTest(PySparkTest):

    def test_transform(self):
        mapping_table = self.spark.createDataFrame([
            (10, 100, True, 0),
            (10, 101, True, 0),
            (10, 102, False, 0),
            (11, 103, True, 0),
            (11, 104, True, 1)
        ], [c.ARTIST_ID, c.ARTIST_COMPOUND_ID, c.MAIN_ARTIST, c.PRIORITY])

        playback_log = self.spark.createDataFrame([
            (1, 10, 1),
            (1, 11, 1),
            (1, 12, 1),
            (2, 11, 1),
            (2, 13, 1)
        ], [c.USER_ID, c.ARTIST_ID, c.COUNT])

        res = MultipleCompoundTransformer(mapping_table).transform(playback_log)

        self.assertEqual({c.USER_ID, c.ARTIST_ID, c.COUNT}, set(res.columns))
        self.assertEqual(8, res.count())
        self.assertEqual(1.0, res.where(F.col(c.ARTIST_ID) == 12).collect()[0][c.COUNT])
        self.assertEqual(0.5, res.where(F.col(c.ARTIST_ID) == 101).collect()[0][c.COUNT])