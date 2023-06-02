from transformers.where_transformer import WhereTransformer
from pyspark_test import PySparkTest

import utils.constants as c
import pyspark.sql.functions as F


class WhereTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, True),
            (2, False),
            (3, True)
        ], [c.PLAYLIST_ID, c.DELETED])

        res = WhereTransformer(~F.col(c.DELETED)).transform(dataset)

        self.assertEqual(1, res.count())
