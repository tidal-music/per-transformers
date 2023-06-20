from tidal_per_transformers.transformers import LoggableTransformer


class AggregateTransformer(LoggableTransformer):
    """
    Group a column by the 'group_by' column and perform aggregation expressions defined in 'agg_exp'.
    This implementation does unfortunately not support multiple aggregations per column!

    :param group_by:    Columns to group the DataFrame by
    :type group_by:     str|list
    :param agg_exp:     The aggregation expression
    :type agg_exp:      dict|list
    """
    def __init__(self, group_by, agg_exp, aliases=None):
        super(AggregateTransformer, self).__init__()
        self.group_by = group_by
        self.agg_exp = agg_exp
        self.aliases = aliases
        if aliases is not None:
            assert len(aliases) == len(agg_exp), "Please provide an alias for every aggregated column"

    def _transform(self, dataset):
        if type(self.agg_exp) is list:
            output = dataset.groupBy(self.group_by).agg(*self.agg_exp)
        else:
            output = dataset.groupBy(self.group_by).agg(self.agg_exp)

        return output if self.aliases is None else self.rename_agg_columns(output)

    def rename_agg_columns(self, dataset):
        columns = {f'{v}({k})': alias for alias, (k, v) in zip(self.aliases, self.agg_exp.items())}
        # rename won't work for count(*) - correct for this
        if 'count(*)' in columns:
            columns['count(1)'] = columns.pop('count(*)')

        for old_name, new_name in columns.items():
            dataset = dataset.withColumnRenamed(old_name, new_name)
        return dataset


