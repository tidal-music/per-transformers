from tidal_per_transformers.transformers import DuplicateSequenceTransformer
import pyspark.sql.functions as F
import tidal_per_transformers.transformers.utils.constants as c
from pyspark_test import PySparkTest
import pyspark.sql.types as T


class DuplicatePlaylistTracksTransformerTest(PySparkTest):

    def test_transform(self):
        array_schema = T.ArrayType(T.StructType([
                T.StructField(c.ARTIST_ID, T.IntegerType()),
                T.StructField(c.TRACK_GROUP, T.IntegerType())]))

        schema = T.StructType([
            T.StructField(c.ID, T.IntegerType()),
            T.StructField(c.TRACKS, array_schema)])

        dataset = self.spark.createDataFrame([
            (0, [{"artistId": 1, "trackGroup": 1002},
                 {"artistId": 3, "trackGroup": 1002},
                 {"artistId": 4, "trackGroup": 1003},
                 {"artistId": 5, "trackGroup": 1004}]),
            (1, [{"artistId": 1, "trackGroup": 1001},
                 {"artistId": 3, "trackGroup": 1004},
                 {"artistId": 6, "trackGroup": 1001},
                 {"artistId": 7, "trackGroup": 1006}]),
            (2, [{"artistId": 6, "trackGroup": 1007},
                 {"artistId": 7, "trackGroup": 1007},
                 {"artistId": 1, "trackGroup": 1007},
                 {"artistId": 3, "trackGroup": 1010}])
        ], schema=schema)

        res = DuplicateSequenceTransformer(c.TRACKS, c.TRACK_GROUP, schema=array_schema).transform(dataset)

        self.assertEqual(3, res.count())
        self.assertEqual(dataset.schema, res.schema)
        self.assertEqual(8, res.select(F.sum(F.size(c.TRACKS))).collect()[0][0])





