from pyspark.sql.types import Row
from tidal_per_transformers.transformers.generic_recommendation_formatter_transformer import \
    GenericRecommendationFormatterTransformer
from pyspark_test import PySparkTest
import tidal_per_transformers.transformers.utils.constants as c


class AggregateTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, [11, 15]),
            (2, [12, 10, 15]),
        ], [c.USER, c.ITEM])

        output = GenericRecommendationFormatterTransformer(group_key=c.USER,
                                                           recommendations_col=c.ITEM,
                                                           item_type=c.TRACK,
                                                           module_name=c.MODULE_ID,
                                                           ).transform(dataset)

        self.assertEqual([
            Row(hashKey="1", moduleId=c.MODULE_ID,
                item=[Row(type=c.TRACK.upper(), id="11"), Row(type=c.TRACK.upper(), id="15")]),
            Row(hashKey="2", moduleId=c.MODULE_ID,
                item=[Row(type=c.TRACK.upper(), id="12"),
                      Row(type=c.TRACK.upper(), id="10"),
                      Row(type=c.TRACK.upper(), id="15")])
        ],
            output.collect())
