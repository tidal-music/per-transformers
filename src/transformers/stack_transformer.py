from pyspark.ml.base import Transformer


class StackTransformer(Transformer):

    def __init__(self,
                 cols_to_fix: list,
                 cols_to_stack: list,
                 output_col_names: list):
        """ Reshape dataframe into long format

        :param cols_to_fix:         columns that should remain
        :param cols_to_stack:       columns to stack. Column values will be stacked into values and column neames will
        be stacked into 'variable' column
        :param output_col_names:    names for value columns, typically [c.VARIABLE, c.VALUE]
        :param value_col_str:       string with names for columns that are stacked
        :param stack_expr:          string with stack expression, e.g.
        "stack(2, 'nStreams', nStreams, 'streamDuration', streamDuration) as (variable, value)"

        Before stacking

        | date       | experimentVariant | nUsers | streamDuration | sessionLength |
        |------------|-------------------|--------|----------------|---------------|
        | 2022-01-01 | control           | 1000   | 6000           | 30000         |
        | 2022-01-01 | test_variant1     | 2000   | 5000           | 49999         |
        | 2022-01-01 | test_variant2     | 2500   | 5000           | 49999         |

        After stacking

        | date       | experimentVariant | nUsers | variable       | value |
        |------------|-------------------|--------|----------------|-------|
        | 2022-01-01 | control           | 1000   | streamDuration | 6000  |
        | 2022-01-01 | test_variant1     | 2000   | streamDuration | 5000  |
        | 2022-01-01 | test_variant2     | 2500   | streamDuration | 5000  |
        | 2022-01-01 | control           | 1000   | sessionLength  | 30000 |
        | 2022-01-01 | test_variant1     | 2000   | sessionLength  | 49999 |
        | 2022-01-01 | test_variant2     | 2500   | sessionLength  | 49999 |

        cols_to_fix = ["date", "experimentVariant", "nUsers"]
        cols_to_stack = ["streamDuration", "sessionLength"]
        output_col_names = ["variable", "value"]
        """
        super().__init__()
        self.cols_to_stack = cols_to_stack
        self.cols_to_fix = cols_to_fix
        self.output_col_names = output_col_names
        assert len(self.output_col_names) == 2, "Should not pass more than 2 values here!"

    def _transform(self, dataset):
        return dataset.selectExpr(*self.cols_to_fix, self._get_stack_expr())

    def _get_stack_expr(self):
        value_col_str = "(" + ','.join(self.output_col_names) + ")"
        return (f'stack({len(self.cols_to_stack)},' + ','.join(f'"{i}", {i}' for i in self.cols_to_stack) + ') as '
                + value_col_str)
