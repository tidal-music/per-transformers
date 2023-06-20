import pyspark.sql.functions as F
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
import tidal_per_transformers.transformers.utils.constants as c


class MinMaxScalingTransformer(LoggableTransformer):
    """
    Min max transformation
    """
    def __init__(self, input_score_range=None, target_score_range=(1, 5), rounding=True):
        super().__init__()
        if input_score_range is not None:
            assert input_score_range[1] > input_score_range[0], "invalid input score range"

        self.input_score_range = input_score_range
        self.target_score_range = target_score_range
        self.rounding = rounding

    def _transform(self, scores):
        if self.input_score_range is None:
            min_max_score = scores.select(F.min(c.SCORE), F.max(c.SCORE)).collect()[0]
            self.input_score_range = min_max_score[0], min_max_score[1]

        input_range = self.input_score_range[1] - self.input_score_range[0]
        output_range = self.target_score_range[1] - self.target_score_range[0]
        scores = (scores
                  .withColumn(c.SCORE, (F.col(c.SCORE) - F.lit(self.input_score_range[0])) / F.lit(input_range))
                  .withColumn(c.SCORE, F.col(c.SCORE) * output_range + self.target_score_range[0]))

        if self.rounding:
            scores = scores.withColumn(c.SCORE, F.round(c.SCORE))
        return scores
