from typing import Optional

import pyspark.sql.functions as F
from pandas import DataFrame
from pyspark.ml import Transformer

import tidal_per_transformers.transformers.utils.constants as c


class VoicenessFilterTransformer(Transformer):

    def __init__(self, voiceness_scores: DataFrame, track_index: DataFrame, score_threshold: Optional[float] = None):
        """
        Transformer used to remove tracks containing voice (non music) content

        :parma voice_scores:     dataframe containing the predicted voiceness scores
        :param track_index:      track index used to map productIds to trackGroup
        :param score_threshold:  custom threshold value used to determine classification (default = 0.5)
        """
        super().__init__()
        self.voiceness_scores = voiceness_scores
        self.product_to_track_group_mapping = track_index.select(F.col(c.ID).alias(c.PRODUCT_ID), c.TRACK_GROUP)
        self.score_threshold = score_threshold

    def _transform(self, dataset: DataFrame) -> DataFrame:
        assert c.TRACK_GROUP in dataset.columns, f"DataFrame must contain a {c.TRACK_GROUP} column"
        return dataset.join(self.get_voice_track_groups(self.voiceness_scores), c.TRACK_GROUP, "left_anti")

    def get_voice_track_groups(self, voiceness: DataFrame) -> DataFrame:
        if self.score_threshold:
            voiceness = voiceness.where(F.col(c.SCORE) >= self.score_threshold)
        else:
            voiceness = voiceness.where(F.col(c.VOICE) == 1)

        return (voiceness
                .join(self.product_to_track_group_mapping, c.PRODUCT_ID)
                .select(c.TRACK_GROUP)
                .distinct())

