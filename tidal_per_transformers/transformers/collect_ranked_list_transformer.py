from pyspark.sql import functions as F
from pyspark.sql.window import Window
from tidal_per_transformers.transformers import LoggableTransformer


class CollectRankedListTransformer(LoggableTransformer):
    """
    Group a column by the 'group_by' column and collect the sorted values in the 'list_col' as a list

    :param group_by:   Columns to group the DataFrame by
    :param list_col:        Column for which we collect the values
    :param sort_by:         The column used to sort the values
    :param alias:           The name given to the new list column
    """
    def __init__(self, group_by, list_col, sort_by, alias):
        super(CollectRankedListTransformer, self).__init__()
        self.group_by = group_by
        self.list_col = list_col
        self.sort_by = sort_by
        self.alias = alias
        assert(isinstance(group_by, list))

    def _transform(self, dataset):
        w = Window.partitionBy(self.group_by).orderBy(self.sort_by)

        return (dataset
                .withColumn(self.alias, F.collect_list(self.list_col).over(w))
                .groupBy(self.group_by)
                .agg(F.max(self.alias).alias(self.alias)))
