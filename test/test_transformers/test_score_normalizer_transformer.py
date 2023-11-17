import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.score_normalizer_transformer import ScoreNormalizerTransformer
from pyspark_test import PySparkTest


class ScoreNormalizerTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, 11, 13),
            (1, 13, 5),
            (2, 12, 12),
            (3, 11, 7),
            (3, 14, 2),
            (3, 16, 3),
            (4, 13, 6),
            (5, 12, 3),
            (6, 13, 7),
        ], [c.USER, c.ITEM, c.SCORE])

        output = (ScoreNormalizerTransformer(partition_by=c.USER, order_by=c.SCORE)
                  .transform(dataset))

        self.assertEqual(9, output.count())
        self.assertEqual({c.USER, c.ITEM, c.SCORE}, set(output.columns))

        user1_item11_score = output.where((F.col(c.USER) == 1) & (F.col(c.ITEM) == 11)).select(c.SCORE).collect()
        user1_item13_score = output.where((F.col(c.USER) == 1) & (F.col(c.ITEM) == 13)).select(c.SCORE).collect()
        user3_item11_score = output.where((F.col(c.USER) == 3) & (F.col(c.ITEM) == 11)).select(c.SCORE).collect()
        user3_item14_score = output.where((F.col(c.USER) == 3) & (F.col(c.ITEM) == 14)).select(c.SCORE).collect()
        user3_item16_score = output.where((F.col(c.USER) == 3) & (F.col(c.ITEM) == 16)).select(c.SCORE).collect()
        self.assertNotEqual(user1_item11_score[0][0], user1_item13_score[0][0])
        self.assertNotEqual(user3_item11_score[0][0], user3_item16_score[0][0])
        self.assertEqual(1, user1_item11_score[0][0])
        self.assertLessEqual(user1_item13_score[0][0], 1)
        self.assertLessEqual(user3_item14_score[0][0], user3_item16_score[0][0])




