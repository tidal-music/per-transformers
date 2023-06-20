import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c

from pyspark.sql import DataFrame
from tidal_per_transformers.transformers import LoggableTransformer


class ParticipatingArtistsTransformer(LoggableTransformer):
    """Transforms artists field into main and featuring artist transformer

    """

    def __init__(self, artist_field: str = 'artists'):
        super().__init__()
        self.main_artist_filter = f"filter({artist_field}, x -> x.main == 'true')"
        self.featuring_artist_filter = f"filter({artist_field}, x -> x.main == 'false')"

    def _transform(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .withColumn(c.MAIN_ARTISTS, F.expr(self.main_artist_filter))
                .withColumn(c.FEATURING_ARTISTS, F.expr(self.featuring_artist_filter))
                )
