from tidal_per_transformers.transformers.union_transformer import UnionTransformer
from pyspark_test import PySparkTest

import tidal_per_transformers.transformers.utils.constants as c


class UnionTransformerTest(PySparkTest):

    def test_transform(self):
        dependent = self.spark.createDataFrame([
            (3, [10, 12, 14]),
            (4, [14, 12, 17]),
        ], [c.PLAYLIST_ID, c.TRACKS])

        independent = self.spark.createDataFrame([
            (1, [10, 11, 12]),
            (2, [10, 13, 16])
        ], [c.PLAYLIST_ID, c.TRACKS])

        res = UnionTransformer(other=independent).transform(dataset=dependent)
        self.assertEqual(4, res.select(c.PLAYLIST_ID).distinct().count())

        # check the order of columns is aligned if they're not in the same order
        dependent1 = self.spark.createDataFrame([
            (3, 10),
            (4, 11),
        ], [c.PLAYLIST_ID, c.TRACKS])

        independent1 = self.spark.createDataFrame([
            (12, 1),
            (13, 2)
        ], [c.TRACKS, c.PLAYLIST_ID])
        res1 = UnionTransformer(other=independent1).transform(dataset=dependent1)
        self.assertEqual(dependent1.columns, res1.columns)
        self.assertEqual([10, 11, 12, 13], [i[c.TRACKS] for i in res1.select(c.TRACKS).distinct().collect()])
