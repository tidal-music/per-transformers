from transformers.top_items_dithering_transformer import TopItemsDitheringTransformer
from pyspark_test import PySparkTest
import pyspark.sql.functions as F
import transformers.utils.constants as c


class TopItemsDitheringTransformerTest(PySparkTest):

    def test_top_items_transformer(self):
        df = self.spark.createDataFrame([
            (1, 1, 10),
            (1, 2, 8),
            (1, 3, 6),
            (1, 4, 4),
            (1, 5, 3),
            (1, 6, 2),
            (1, 7, 2),
            (1, 8, 1),
            (2, 1, 20),
            (2, 2, 11),
            (2, 3, 8),
            (2, 4, 7),
        ], [c.USER, c.TRACK_GROUP, c.COUNT])

        top = TopItemsDitheringTransformer([c.USER], F.desc(c.COUNT), 3).transform(df)

        assert 6 == top.count()
        assert df.columns == top.columns

        top_with_rn = TopItemsDitheringTransformer([c.USER], F.desc(c.COUNT), 2, keep_rank_col=True).transform(df)

        assert 4 == top_with_rn.count()
        assert df.columns + ["rn"] == top_with_rn.columns