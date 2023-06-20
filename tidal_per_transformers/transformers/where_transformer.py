from typing import Dict
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class WhereTransformer(LoggableTransformer):
    def __init__(self, where_filter):
        super().__init__()
        self.where_filter = where_filter

    def _transform(self, dataset):
        return dataset.where(self.where_filter)

    def get_loggable_dict(self, invocation_order: int) -> Dict:
        """ Returns a params dictionary that can be logged using TransformPipeLine class, with params items_column
            and field_column
        :param invocation_order: the invocation order of the transformer
        :return: loggable params dictionary
        """
        return {f'Transformer_{type(self).__name__}_{invocation_order}': {"where_clause": f'{self.where_filter}'}}
