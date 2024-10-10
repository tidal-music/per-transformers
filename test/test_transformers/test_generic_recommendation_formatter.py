from pyspark.sql.types import Row
from tidal_per_transformers.transformers.generic_recommendation_formatter import GenericRecommendationFormatter
from pyspark_test import PySparkTest
import tidal_per_transformers.transformers.utils.constants as c


class AggregateTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, [11, 15]),
            (2, [12, 10, 15]),
        ], [c.USER, c.ITEM])

        output = GenericRecommendationFormatter(hash_key=c.USER,
                                                recommendations_col=c.ITEM,
                                                item_type=c.TRACK_GROUP,
                                                module_name=c.MODULE_ID,
                                                module_version="1"
                                                ).transform(dataset)

        self.assertEqual([
            Row(hashKey=1, moduleId=f'{c.MODULE_ID}_v1',
                item=[Row(type='trackGroup', id=11), Row(type='trackGroup', id=15)]),
            Row(hashKey=2, moduleId=f'{c.MODULE_ID}_v1',
                item=[Row(type='trackGroup', id=12), Row(type='trackGroup', id=10), Row(type='trackGroup', id=15)])
            ],
            output.collect())
