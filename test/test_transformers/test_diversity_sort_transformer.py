from pyspark import Row

import tidal_per_transformers.transformers.utils.constants as c
from pyspark_test import PySparkTest

from tidal_per_transformers.transformers.artifact_id_mapping_by_user_country_transformer import \
    ArtifactIdMappingByUserCountryTransformer
from tidal_per_transformers.transformers.diversity_sort_transformer import DiversitySortTransformer


class DiversitySortTransformerTest(PySparkTest):

    def test_transform(self):
        df = self.spark.createDataFrame([
            (1, 'rock', 101, 1),
            (1, 'rock', 102, 2),
            (1, 'pop', 102, 3),
            (1, 'pop', 103, 4),
            (1, 'country', 104, 5),
        ], [c.USER_ID, c.GENRE, c.ARTIST_ID, c.RN])

        res = DiversitySortTransformer(
            id_col=[c.USER_ID],
            partition_one=[c.GENRE],
            partition_two=[c.ARTIST_ID],
            order_by=c.RN,
            gap=2,
        ).transform(df).sort(c.RANK).collect()
        self.assertEqual([Row(userId=1, genre='rock', artistId=101, rn=1, rank=1),
                          Row(userId=1, genre='country', artistId=104, rn=5, rank=2),
                          Row(userId=1, genre='rock', artistId=102, rn=2, rank=3),
                          Row(userId=1, genre='pop', artistId=103, rn=4, rank=4),
                          Row(userId=1, genre='pop', artistId=102, rn=3, rank=5)], [row for row in res])
