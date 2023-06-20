from tidal_per_transformers.transformers.stack_transformer import StackTransformer
from pyspark_test import PySparkTest

import tidal_per_transformers.transformers.utils.constants as c


class StackTransformerTest(PySparkTest):

    def test_transform(self):
        df = self.spark.createDataFrame([
            (1, 'a', 1, 20, 20),
            (1, 'b', 2, 15, 20),
            (1, 'a', 3, 30, 20),
            (1, 'a', 3, 46, 20),
            (2, 'b', 4, 10, 20),
            (2, 'a', 4, 20, 20),
            (2, 'a', 4, 33, 20),
            (2, 'a', 4, 40, 20),
        ], [c.DATE, c.NAME, c.TRACKS, c.POPULARITY, c.MATCHES])

        res = StackTransformer(cols_to_fix=[c.DATE, c.NAME],
                               cols_to_stack=[c.TRACKS, c.POPULARITY],
                               output_col_names=[c.VARIABLE, c.VALUE]).transform(df)
        self.assertEqual(16, res.count())
        self.assertEqual({c.VARIABLE, c.NAME, c.DATE, c.VALUE}, set(res.columns))
        self.assertEqual([c.POPULARITY, c.TRACKS], [r[c.VARIABLE] for r in res.select(c.VARIABLE).distinct().collect()])

    def test_get_stack_expr(self):
        stack_exp = (StackTransformer(cols_to_fix=[c.DATE, c.NAME],
                                      cols_to_stack=[c.TRACKS, c.POPULARITY],
                                      output_col_names=[c.VARIABLE, c.VALUE])
                     ._get_stack_expr())
        self.assertEqual('stack(2,"tracks", tracks,"popularity", popularity) as (variable,value)', stack_exp)
