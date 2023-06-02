import utils.constants as c
from transformers import VoicenessFilterTransformer
from pyspark_test import PySparkTest


class VoicenessFilterTransformerTest(PySparkTest):

    def test_transform(self):
        voiceness_scores = self.spark.createDataFrame([
            (1, 0.03, 0),
            (2, 0.33, 0),
            (3, 0.53, 1),
            (4, 0.88, 1),
            (5, 0.99, 1),
        ], [c.PRODUCT_ID, c.SCORE, c.VOICE])

        track_index = self.spark.createDataFrame([
            (1, 11),
            (2, 12),
            (3, 13),
            (4, 14),
            (5, 15),
        ], [c.ID, c.TRACK_GROUP])

        tracks = self.spark.createDataFrame([
            (11,),
            (12,),
            (13,),
            (14,),
            (15,),
            (16,)
        ], [c.TRACK_GROUP])

        res = VoicenessFilterTransformer(voiceness_scores, track_index).transform(tracks)

        self.assertEqual(tracks.schema, res.schema)
        self.assertEqual(3, res.count())

    def test_transform_custom_threshold(self):
        voiceness_scores = self.spark.createDataFrame([
            (1, 0.03, 0),
            (2, 0.33, 0),
            (3, 0.53, 1),
            (4, 0.88, 1),
            (5, 0.99, 1),
        ], [c.PRODUCT_ID, c.SCORE, c.VOICE])

        track_index = self.spark.createDataFrame([
            (1, 11),
            (2, 12),
            (3, 13),
            (4, 14),
            (5, 15),
        ], [c.ID, c.TRACK_GROUP])

        tracks = self.spark.createDataFrame([
            (11,),
            (12,),
            (13,),
            (14,),
            (15,),
            (16,)
        ], [c.TRACK_GROUP])

        res = VoicenessFilterTransformer(voiceness_scores, track_index, 0.9).transform(tracks)

        self.assertEqual(tracks.schema, res.schema)
        self.assertEqual(5, res.count())
