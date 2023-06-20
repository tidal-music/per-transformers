import math

import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType


def dither(dataset, rank_col, partition_by, epsilon=2.0, start_pos=0, dither_col=None):
    """
    Dither the 'rank_col' in the DataFrame and returns the dithering ordering.

    :param dataset:         The dataset containing the original ordering
    :type dataset:          pyspark.sql.DataFrame
    :param rank_col:        The original rank
    :param partition_by:    The partition we want to dither
    :type partition_by:     list(str)
    :param epsilon:         Controls the amount of dithering applied (range 1.25 - 2.0)
    :param start_pos:       Controls the amount of dithering applied to the top items (higher = more)
    :param dither_col:      Column name to put the dithered number
    :rtype:                 pyspark.sql.DataFrame
    """
    sd = dither_sd(epsilon)

    if dither_col is None:
        dither_col = rank_col

    return (dataset
            .withColumn("sd", F.lit(sd))
            .withColumn("dither_score", dither_score_udf(rank_col, "sd", F.lit(start_pos)))
            .withColumn(dither_col, F.row_number().over(Window.partitionBy(partition_by).orderBy("dither_score")))
            .drop("sd", "dither_score"))


def dither_sd(epsilon):
    return math.sqrt(np.log(epsilon)) if epsilon > 1.0 else 1e-10


def dither_score(pos, sd, x_start=0):
    """ Generate a dithering score that determines the new order of the rows.
    Source: https://buildingrecommenders.wordpress.com/2015/11/11/dithering/

    :param pos:       The original position in the mix (starting from 1)
    :param sd:        The standard deviation of the distribution
    :param x_start:   The starting position on the x-axis
    """
    return float((np.log(x_start + pos) + np.random.normal(0.0, sd, 1))[0])


dither_score_udf = F.udf(dither_score, DoubleType())
