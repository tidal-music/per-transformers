from pyspark.sql import Window, DataFrame, functions as F
from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
import tidal_per_transformers.transformers.utils.constants as c


class PlaylistCoherenceTransformer(LoggableTransformer):
    """
    Filter playlists based on how coherence according to the playlist_coherence model

    threshold: minimum coherence value to keep the playlist
    playlist_coherence_model: model used to compute the coherence
    """
    PERCENTILE = "percentile"

    def __init__(self, threshold: float, playlist_coherence_model: DataFrame):
        super().__init__()
        self.threshold = threshold
        self.model = playlist_coherence_model
        self.model = self.model.withColumn(c.ID, F.col(c.PLAYLIST_ID)) if c.ID not in self.model.columns else self.model

    def _transform(self, dataset):
        dataset = dataset.withColumn(c.ID, F.col(c.PLAYLIST_ID)) if c.ID not in dataset.columns else dataset

        non_coherent = (self.model
                        .withColumn(self.PERCENTILE, F.percent_rank().over(Window.orderBy(c.SCORE)))
                        .where(F.col(self.PERCENTILE) > self.threshold)
                        .select(F.col(c.ID)))

        return dataset.join(non_coherent, c.ID, "left_anti")
