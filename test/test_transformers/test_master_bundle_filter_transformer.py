import tidal_per_transformers.transformers.utils.constants as c
from pyspark_test import PySparkTest
from tidal_per_transformers.transformers.master_bundle_filter_transformer import MasterBundleFilterTransformer


class MasterBundleFilterTransformerTest(PySparkTest):

    def test_transform(self):
        albums = self.spark.createDataFrame([
            ("a",),
            ("b",),
            ("c",),
            ("d",),
            ("e",),
            ("f",),
            ("g",),
            ("h",),
            ("x",),
        ], [c.MASTER_BUNDLE_ID])

        filters = self.spark.createDataFrame([
            ("a", True, "EP", 10_000, 5_000, 0, 0, 0, 0, 0),
            ("b", True, "ALBUM", 100, 20, 0, 0, 0, 0, 0),  # low stream counts
            ("c", True, "ALBUM", 10_000, 5_000, 0, 0, 0, 0, 0),
            ("d", True, "SINGLE", 10_000, 5_000, 0, 0, 0, 0, 0),
            ("e", True, "ALBUM", 10_000, 5_000, 1, 0, 0, 0, 0),
            ("f", True, "ALBUM", 10_000, 5_000, 0, 1, 0, 0, 0),
            ("g", True, "ALBUM", 10_000, 5_000, 0, 0, 1, 0, 0),
            ("h", True, "ALBUM", 10_000, 5_000, 0, 0, 0, 1, 0),
            ("i", True, "ALBUM", 10_000, 5_000, 0, 0, 0, 0, 1),
        ], [c.MASTER_BUNDLE_ID,
            c.AVAILABLE,
            c.ALBUM_TYPE,
            c.STREAM_COUNT,
            c.STREAMERS_COUNT,
            c.CHILDREN,
            c.HOLIDAY,
            c.AMBIENT,
            c.NON_MUSIC,
            c.VARIANT])

        res = MasterBundleFilterTransformer(filters,
                                            min_album_streamers=200,
                                            min_album_streams=500,
                                            remove_holiday_music=True,
                                            remove_ambient_music=True,
                                            remove_children_music=True,
                                            remove_variant_versions=True).transform(albums).collect()

        self.assertEqual(2, len(res))
        [self.assertIn(x[0], ("c", "x")) for x in res]

