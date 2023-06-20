from abc import ABC
from typing import Dict
from pyspark.ml.base import Transformer
import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.utils.class_utils import as_dict


class LoggableTransformer(Transformer, ABC):
    """
    Collect list on "item_column" and ordered by order_by_column
    """
    def __init__(self):
        super(LoggableTransformer, self).__init__()

    def get_loggable_dict(self, invocation_order: int) -> Dict:
        """
        Collects the loggable dict for the given transformer with its invocation order.
        :param invocation_order: the invocation order of the given transformer
        :return: loggable params dictionary
        """
        params = as_dict(self)
        [params.pop(x, None) for x in [c.UID, c.HIDDEN_PARAMS]]
        return {f'Transformer_{type(self).__name__}_{invocation_order}': params}
