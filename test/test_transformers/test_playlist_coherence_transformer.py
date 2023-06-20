from transformers.playlist_coherence_transformer import PlaylistCoherenceTransformer
from pyspark_test import PySparkTest

import transformers.utils.constants as c


class TestPlaylistCoherenceTransformer(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1,  100),
            (2, 200),
        ], [c.PLAYLIST_ID, c.USER_ID])

        playlist_coherence = self.spark.createDataFrame([
            (1, 0.95),
            (2, 0.55),
        ], [c.PLAYLIST_ID, c.SCORE])

        res = PlaylistCoherenceTransformer(0.9, playlist_coherence).transform(dataset).collect()
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0][c.ID], 2)
        self.assertEqual(res[0][c.PLAYLIST_ID], 2)
        self.assertEqual(res[0][c.USER_ID], 200)

