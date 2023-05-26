from transformers.top_items_transformer import TopItemsTransformer
from pyspark_test import PySparkTest
import pyspark.sql.functions as F


class TransformerTest(PySparkTest):

    def test_top_items_transformer(self):
        df = self.sc.createDataFrame([
            (1, 2, 1),  # 2 neighbours from artist 2
            (1, 2, 2),
            (1, 3, 3),  # 2 neighbours from artist 3
            (1, 3, 4),
            (2, 4, 1),  # New track, four neighbours from artist 4
            (2, 4, 2),
            (2, 4, 3),
            (2, 4, 4),
        ], ["id", "artistId", "pos"])

        top1 = TopItemsTransformer(['id', 'artistId'], 'pos', 1).transform(df)
        assert (top1.count() == 3)
        assert (len(top1.columns) == 3)
        assert (top1.columns == ['id', 'artistId', 'pos'])

        top2 = TopItemsTransformer(['id', 'artistId'], 'pos', 2).transform(df)
        assert (top2.count() == 6)
        assert (top2.agg(F.max("pos") == 2))

        top3 = TopItemsTransformer('id', F.col('pos').desc(), 3).transform(df)
        assert (top3.count() == 6)
        assert (top3.agg(F.max("pos") == 4))

        top4 = TopItemsTransformer(partition='id', order_by='artistId', n=1, ranking_function=F.dense_rank,
                                   keep_rnk_col=True).transform(df)
        assert (top4.count() == 6)
        assert (top4.agg(F.max("pos") == 1))
        assert (len(top4.columns) == 4)

