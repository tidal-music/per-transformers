from itertools import chain

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql import Window
from pyspark.sql.types import MapType, StringType


def get_top_items(df, partition, order, n):
    """
    Get the top 'n' items per partitions from the given ordering

    :param df:          DataFrame to search
    :param partition:   Column(s) to partition by
    :param order:       Column(s) to order by
    :param n:           Number of top items to return per partition
    :return:            DataFrame containing the top n items per partition
    """
    window = Window.partitionBy(partition).orderBy(order)
    return df.withColumn("rn", F.row_number().over(window)).where(F.col("rn") <= n).drop("rn")


def list_to_index_map(l):
    return {str(i): l[i] for i in range(0, len(l))}


list_to_top_list = F.udf(list_to_index_map, MapType(StringType(), StringType()))


def dict_to_map_column(mapping: dict) -> Column:
    """
    Function to create map from dictionary, used e.g. to create a column from a dictionary.

    :param mapping: dictionary of key value pairs to be used to create new column.
    :return column
    """
    return F.create_map([F.lit(x) for x in chain(*mapping.items())])
