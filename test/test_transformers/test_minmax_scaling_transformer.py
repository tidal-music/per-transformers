import pyspark.sql.functions as F

import utils.constants as c
from transformers.minmax_scaling_transformer import MinMaxScalingTransformer
from pyspark_test import PySparkTest


class MinMaxScalingTransformerTest(PySparkTest):

    def test_transform_target_range_0_1(self):
        df = self.sc.createDataFrame([
            (1, 2),
            (2, 5)
        ], [c.ITEM, c.SCORE])
        res = MinMaxScalingTransformer(input_score_range=(2, 5), target_score_range=(0, 1)).transform(df)
        assert res.where(F.col(c.ITEM) == 1).select(c.SCORE).collect()[0][0] == 0
        assert res.where(F.col(c.ITEM) == 2).select(c.SCORE).collect()[0][0] == 1

    def test_transform_target_range_1_5(self):
        df = self.sc.createDataFrame([
            (1, 2),
            (2, 5)
        ], [c.ITEM, c.SCORE])
        res = MinMaxScalingTransformer(input_score_range=(2, 5), target_score_range=(1, 5)).transform(df)
        assert res.where(F.col(c.ITEM) == 1).select(c.SCORE).collect()[0][0] == 1
        assert res.where(F.col(c.ITEM) == 2).select(c.SCORE).collect()[0][0] == 5

    def test_transform_none_input_score_range(self):
        df = self.sc.createDataFrame([
            (1, 2),
            (2, 5)
        ], [c.ITEM, c.SCORE])
        res = MinMaxScalingTransformer(input_score_range=None, target_score_range=(1, 5)).transform(df)
        assert res.where(F.col(c.ITEM) == 1).select(c.SCORE).collect()[0][0] == 1
        assert res.where(F.col(c.ITEM) == 2).select(c.SCORE).collect()[0][0] == 5

    def test_transform_no_rounding(self):
        dataset = self.sc.createDataFrame([
            (0, 0, 1.0),
            (0, 0, 2.0),
            (0, 0, 3.0),
            (0, 0, 4.0),
            (0, 0, 5.0),
        ], [c.USER, c.ITEM, c.SCORE])

        output = MinMaxScalingTransformer(input_score_range=(1, 5), target_score_range=(0.1, 1.0), rounding=False).transform(dataset)
        minmax = output.select(F.max(c.SCORE), F.min(c.SCORE)).collect()[0]

        assert output.count() == 5
        assert set(output.columns) == set(dataset.columns)
        assert minmax[0] == 1.0
        assert minmax[1] == 0.1
