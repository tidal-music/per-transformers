from pyspark.sql import functions as F
from pyspark.sql.types import MapType, ArrayType, StringType
from pyspark.sql.window import Window
from tidal_per_transformers.transformers import LoggableTransformer


class CollectRankedMapTransformer(LoggableTransformer):
    """
    Group a column by the 'group_by' column and collect the sorted values in the 'map_col' as a map

    :param group_by:        Columns to group the DataFrame by
    :param map_col:         Column for which we collect the values
    :param sort_col:        The column used to sort the values
    :param alias:           The name given to the new list column
    :param ascending:       Boolean to sort ascending ot descending
    :param limit:           Number of elements to keep in the list
    """

    def __init__(self,
                 group_by: any,
                 map_col: str,
                 sort_col: str,
                 ascending: bool = True,
                 alias: str = None,
                 limit=10_000):
        super().__init__()
        self.group_by = group_by
        self.sort_col = sort_col
        self.map_col = map_col
        self.alias = alias if alias else map_col
        self.ascending = ascending
        self.limit = limit

    def _transform(self, dataset):
        w = Window.partitionBy(self.group_by).orderBy(self.sort_col if self.ascending else F.desc(self.sort_col))

        return (dataset
                .withColumn(self.alias, F.collect_list(F.struct(self.map_col, self.sort_col)).over(w))
                .groupBy(self.group_by)
                .agg(F.max(self.alias).alias(self.alias))
                .withColumn(self.alias, F.slice(F.from_json(F.to_json(self.alias),
                                                            ArrayType(MapType(StringType(), StringType()))),
                                                1, self.limit))
                )
