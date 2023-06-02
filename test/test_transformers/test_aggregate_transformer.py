from pyspark_test import PySparkTest
from transformers.aggregate_transformer import AggregateTransformer
import utils.constants as c


class AggregateTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, 10, 100),
            (1, 10, 200),
            (1, 10, 300),
            (1, 11, 100),
            (1, 12, 900)
        ], [c.USER, c.ITEM, c.SCORE])

        # Single column group by and aggregation
        output = (AggregateTransformer(c.USER, {c.ITEM: 'count'})
            .transform(dataset)
            .collect())

        assert (1 == len(output))
        assert (5 == output[0]['count(item)'])

        # Multi column group by and single aggregation
        output = (AggregateTransformer([c.USER, c.ITEM], {c.SCORE: 'sum', c.ITEM: 'count'})
            .transform(dataset)
            .orderBy(c.USER, c.ITEM)
            .collect())

        assert (3 == len(output))
        assert (600 == output[0]['sum(score)'])
        assert (3 == output[0]['count(item)'])

        # Columns are successfully renamed to their aliases
        output = (AggregateTransformer([c.USER], {c.SCORE: 'sum', c.ITEM: 'count'}, ['sum', 'count'])
            .transform(dataset)
            .collect())

        assert(1 == len(output))
        assert (1600 == output[0]['sum'])
        assert (5 == output[0]['count'])

        #test count(*) case
        output = AggregateTransformer([c.USER, c.ITEM], {'*': 'count'}, aliases=['test']).transform(dataset)
        assert ['user', 'item', 'test'] == output.columns
