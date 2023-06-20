from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class SelectTransformer(LoggableTransformer):
    def __init__(self, selected_columns):
        super().__init__()
        self.selected_columns = selected_columns

    def _transform(self, dataset):
        return dataset.select(self.selected_columns)
