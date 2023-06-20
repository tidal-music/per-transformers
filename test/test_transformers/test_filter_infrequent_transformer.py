from transformers.filter_infrequent_transformer import FilterInfrequentTransformer
from pyspark_test import PySparkTest
import transformers.utils.constants as c


class FilterInfrequentTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, "SONGWRITER", 10),
            (1, "SONGWRITER", 11),
            (1, "SONGWRITER", 12),
            (1, "PRODUCER", 10),
            (2, "SONGWRITER", 10),
            (3, "PRODUCER", 10),
        ], [c.CONTRIBUTOR_ID, c.CONTRIBUTOR_TYPE, c.TRACK_GROUP])

        output = FilterInfrequentTransformer([c.CONTRIBUTOR_ID, c.CONTRIBUTOR_TYPE], 2).transform(dataset)

        assert 3 == output.count()
        assert set(dataset.columns) == set(output.columns)