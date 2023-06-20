from typing import Optional, List, Union

from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class PivotTransformer(LoggableTransformer):

    def __init__(self,
                 group_by: Union[str, List],
                 pivot_col: str,
                 agg_exp,
                 values: Optional[List] = None):
        """
        Pivots a column of the current DataFrame and perform the specified aggregation

        :param group_by:    Columns to group the DataFrame by
        :param pivot_col:   Column used to pivot
        :param agg_exp:     The aggregation expression
        :param values:      A list of distinct values to pivot on
        """

        super().__init__()
        self.group_by = group_by
        self.pivot_col = pivot_col
        self.values = values
        self.agg_exp = agg_exp

    def _transform(self, dataset):
        return (dataset
                .groupBy(self.group_by)
                .pivot(self.pivot_col, self.values)
                .agg(self.agg_exp))
