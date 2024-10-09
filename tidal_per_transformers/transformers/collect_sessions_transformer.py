from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window
from tidal_per_transformers.transformers import LoggableTransformer


class CollectSessionsTransformer(LoggableTransformer):
    """
    Given a group of scored and time-sorted sessions by the users, it will collect
    them into 2 lists: recent sessions and relevant sessions. Both lists will be
    trimmed to the given limit.

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

    def _group_collect_recency_ordered(self, dataset: DataFrame) -> DataFrame:
        """
        Recent streaming sessions are composed of last X streamed tracks, with last
        listened item in last position.

        This gets built by going through user's sessions on an ascending fashion (most recent
        processed last) and incrementally collecting items, to then fetch the resulting list
        from the last instance.

        Note: due to the usage of F.last(), this function is not deterministic
        """
        window_session = Window.partitionBy(self.group_by).orderBy(F.col(self.order_by_session))
        return dataset.withColumn(
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
        ).groupBy(F.col(self.group_by)).agg(
            F.last(F.col(self.recent_alias)).alias(self.recent_alias)
        )

    def _group_collect_relevance_ordered(self, dataset: DataFrame) -> DataFrame:
        """
        Relevant streaming sessions are composed by grouping of all sessions ordered by
        their relevance score, with the top session last and its first song in last position.

        This gets built by going through scored user's sessions descending in relevance (lowest
        scored processed last) and incrementally collecting items, to then fetch the resulting
        list from the last instance.

        Note: due to the usage of F.last(), this function is not deterministic
        """
        window_score = Window.partitionBy(self.group_by).orderBy(F.desc(F.col(self.order_by_score)))
        return dataset.withColumn(
            self.relevant_alias,
            F.reverse(
                F.slice(
                    F.flatten(F.collect_list(self.list_col).over(window_score)),
                    1, self.limit,
                )
            )
        ).groupBy(F.col(self.group_by)).agg(
            F.last(F.col(self.relevant_alias)).alias(self.relevant_alias)
        )

    def _transform(self, dataset: DataFrame) -> DataFrame:
        recent = self._group_collect_recency_ordered(dataset)
        relevant = self._group_collect_relevance_ordered(dataset)
        return recent.join(relevant, on=self.group_by)
