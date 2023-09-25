from pyspark.ml import Transformer
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c


class FilterInactiveUserTransformer(Transformer):

    def __init__(self, max_inactivity_months):
        super(FilterInactiveUserTransformer, self).__init__()
        self.max_inactivity_months = max_inactivity_months

    def _transform(self, dataset):
        if self.max_inactivity_months is None or self.max_inactivity_months == 0:
            return dataset

        window_spec = Window.partitionBy(c.USER_ID)

        return (dataset
            .withColumn("lastActiveDate", F.max(c.LAST_STREAMED_DATE).over(window_spec))
            .where(F.col("lastActiveDate") >= datetime.now() - timedelta(days=self.max_inactivity_months*30))
            .drop("lastActiveDate"))