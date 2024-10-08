from pyspark.sql import functions as F
from pyspark.sql.window import Window
from tidal_per_transformers.transformers import LoggableTransformer


class CollectSessionsTransformer(LoggableTransformer):
    """
    Given a group of scored and time-sorted sessions by the users, it will collect them into 2 lists:
    - Recent sessions: last X streamed tracks, with last listened item in last position
    - Relevant sessions: grouping of all sessions ordered by their score, with the top session last and its first song in last position
    Both lists will be trimmed to the given limit.

    :param group_by: Columns to group the DataFrame by
    :param order_by_session: The column used to sort the values by time
    :param order_by_score: The column used to sort the values by score
    :param recent_alias: The name given to the new list column ordered by recency
    :param relevant_alias: The name given to the new list column ordered by relevance
    :param list_col: Column for which we collect the values
    :limit: Maximum number of items in the list
    """

    def __init__(self,
                 group_by: str,
                 order_by_session: str,
                 order_by_score: str,
                 recent_alias: str,
                 relevant_alias: str,
                 list_col: str,
                 limit: int = 300):
        super(CollectSessionsTransformer, self).__init__()
        self.group_by = group_by
        self.order_by_session = order_by_session
        self.order_by_score = order_by_score
        self.recent_alias = recent_alias
        self.relevant_alias = relevant_alias
        self.list_col = list_col
        self.limit = limit

    def _transform(self, dataset):
        window_score = Window.partitionBy(self.group_by).orderBy(
            F.desc(F.col(self.order_by_score))
        )
        window_session = Window.partitionBy(self.group_by).orderBy(F.col(self.order_by_session))

        return (
            dataset.withColumn(
                self.recent_alias,
                F.reverse(
                    F.slice(
                        F.reverse(
                            F.flatten(
                                F.collect_list(self.list_col).over(window_session)
                            )
                        ),
                        1, self.limit,
                    )
                ),
            )
            .withColumn(
                self.relevant_alias,
                F.reverse(
                    F.slice(
                        F.flatten(F.collect_list(self.list_col).over(window_score)),
                        1, self.limit,
                    )
                ),
            )
            .groupBy(self.group_by)
            .agg(
                F.max(self.recent_alias).alias(self.recent_alias),
                F.max(self.relevant_alias).alias(self.relevant_alias),
            )
        )