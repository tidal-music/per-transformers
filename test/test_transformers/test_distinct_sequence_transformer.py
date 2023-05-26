from transformers import DistinctSequenceTransformer
from pyspark_test import PySparkTest
import utils.constants as c
import pyspark.sql.types as T


class DistinctSequenceTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.sc.createDataFrame([
            (0, [1, 2, 3, 4, 5]),
            (1, [2, 1, 3, 4, 5]),
            (2, [3, 1, 2, 4, 5])
        ], [c.ID, c.TRACKS])

        res = DistinctSequenceTransformer(c.TRACKS).transform(dataset)

        self.assertEqual(1, res.count())
        self.assertEqual(dataset.schema, res.schema)

    def test_transform_list_structs(self):
        schema = T.StructType([
            T.StructField(c.ID, T.IntegerType()),
            T.StructField(c.TRACKS, T.ArrayType(T.StructType([
                T.StructField(c.ARTIST_ID, T.IntegerType()),
                T.StructField(c.TRACK_GROUP, T.IntegerType())])))])

        dataset = self.sc.createDataFrame([
            (0, [{"artistId": 1, "trackGroup": 1002},
                 {"artistId": 3, "trackGroup": 1002},
                 {"artistId": 4, "trackGroup": 1003},
                 {"artistId": 5, "trackGroup": 1004}]),
            (1, [{"artistId": 1, "trackGroup": 1001},
                 {"artistId": 3, "trackGroup": 1004},
                 {"artistId": 6, "trackGroup": 1005},
                 {"artistId": 7, "trackGroup": 1006}]),
            (2, [{"artistId": 6, "trackGroup": 1007},
                 {"artistId": 7, "trackGroup": 1008},
                 {"artistId": 1, "trackGroup": 1009},
                 {"artistId": 3, "trackGroup": 1010}])
        ], schema=schema)

        res = DistinctSequenceTransformer(f"{c.TRACKS}.{c.ARTIST_ID}").transform(dataset)

        self.assertEqual(2, res.count())
        self.assertEqual(dataset.schema, res.schema)
