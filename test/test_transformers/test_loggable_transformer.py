from tidal_per_transformers.transformers.select_transformer import SelectTransformer
from pyspark_test import PySparkTest
import tidal_per_transformers.transformers.utils.constants as c


class TestLoggableTransformer(PySparkTest):

    def test_transform(self):
        t = SelectTransformer([c.PLAYLIST_ID, c.TRACKS])

        self.assertEqual({'Transformer_SelectTransformer_0': {
            'selected_columns': "['playlistId', 'tracks']"}}, t.get_loggable_dict(0))
