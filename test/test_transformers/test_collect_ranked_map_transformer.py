from pyspark.sql.types import Row
from transformers.collect_ranked_map_transformer import CollectRankedMapTransformer
from pyspark_test import PySparkTest
import transformers.utils.constants as c


class AggregateTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, 10, 1),
            (1, 11, 2),
            (2, 12, 3),
            (2, 13, 4),
            (2, 14, 5)
        ], [c.USER, c.ITEM, c.SCORE])

        output = CollectRankedMapTransformer(group_by=c.USER,
                                             map_col=c.ITEM,
                                             sort_col=c.SCORE,
                                             alias=c.COUNT,
                                             ascending=False,
                                             limit=2).transform(dataset)

        self.assertEqual([Row(user=1, count=[{c.SCORE: '2', c.ITEM: '11'},
                                             {c.SCORE: '1', c.ITEM: '10'}]),
                          Row(user=2, count=[{c.SCORE: '5', c.ITEM: '14'},
                                             {c.SCORE: '4', c.ITEM: '13'}])],
                         output.collect())
