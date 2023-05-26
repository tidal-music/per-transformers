from pyspark.sql import DataFrame

import utils.constants as c
from pyspark.ml import Transformer
import pyspark.sql.functions as F


MAIN_ARTIST_FILTER = "filter(artists, x -> x.main == 'true')"


class MainArtistFromArtistsTransformer(Transformer):
    """ Transformer used to extract the main artist id from the artists field from the index tables """
    def __init__(self, output_column: str = c.ARTIST_ID):
        super(MainArtistFromArtistsTransformer, self).__init__()
        self.output_column = output_column

    def _transform(self, dataset):
        return self.extract_main_artist(dataset)

    def extract_main_artist(self, dataset: DataFrame) -> DataFrame:
        return (dataset
                .withColumn(self.output_column, F.expr(MAIN_ARTIST_FILTER).getItem(0).getField(c.ID))
                .withColumn(self.output_column,
                            F.when(F.col(self.output_column).isNull(), F.col(c.ARTISTS).getItem(0).getField(c.ID))
                            .otherwise(F.col(c.ARTIST_ID))))
