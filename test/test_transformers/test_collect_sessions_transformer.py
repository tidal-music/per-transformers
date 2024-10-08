from pyspark.sql.types import Row
from tidal_per_transformers.transformers.collect_sessions_transformer import CollectSessionsTransformer
from pyspark_test import PySparkTest
import tidal_per_transformers.transformers.utils.constants as c


class AggregateTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, [10, 15], 1, 10),
            (1, [11, 15], 2, 6),
            (2, [12], 1, 1),
            (2, [13, 10, 11], 2, 9),
            (2, [14, 13, 12, 11], 3, 4)
        ], [c.USER, c.ITEM, c.ID, c.SCORE])

        output = CollectSessionsTransformer(group_by=c.USER,
                                            order_by_session=c.ID,
                                            order_by_score=c.SCORE,
                                            recent_alias=c.RECENT,
                                            relevant_alias=c.RELEVANT,
                                            list_col=c.ITEM,
                                            limit=3,).transform(dataset)

        self.assertEqual([Row(user=1, recent=[15, 11, 15], relevant=[15, 10]),
                         Row(user=2, recent=[13, 12, 11], relevant=[11, 10, 13]),
                          ],
                         output.collect())
