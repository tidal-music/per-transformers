import pyspark.sql.functions as F

import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.track_group_filter_transformer import TrackGroupFilterTransformer
from pyspark_test import PySparkTest


class TrackGroupFilterTransformerTest(PySparkTest):

    def test_transform(self):
        track_groups = self.spark.createDataFrame([
            ("a",),
            ("b",),
            ("c",),
            ("d",),
            ("e",),
            ("f",),
            ("g",),
            ("h",),
            ("x",),
        ], [c.TRACK_GROUP])

        filters = self.get_track_group_content_filter_table()

        res = TrackGroupFilterTransformer(filters,
                                          remove_holiday_music=True,
                                          remove_ambient_music=True,
                                          remove_children_music=True,
                                          min_track_duration=60,
                                          max_track_duration=1200,
                                          min_track_streamers=200,
                                          min_track_streams=500).transform(track_groups)

        self.assertEqual(2, res.count())
        self.assertEqual(track_groups.columns, res.columns)

        res_with_children = TrackGroupFilterTransformer(filters,
                                                        remove_holiday_music=True,
                                                        remove_ambient_music=True,
                                                        remove_children_music=False,
                                                        min_track_duration=60,
                                                        max_track_duration=1200,
                                                        min_track_streamers=200,
                                                        min_track_streams=500).transform(track_groups)

        self.assertEqual(3, res_with_children.count())

        res_no_filters = TrackGroupFilterTransformer(filters,
                                                     remove_holiday_music=False,
                                                     remove_ambient_music=False,
                                                     remove_children_music=False,
                                                     min_track_duration=0,
                                                     max_track_duration=9999,
                                                     min_track_streamers=0,
                                                     min_track_streams=0).transform(track_groups)

        self.assertEqual(8, res_no_filters.count())
        self.assertEqual(0, res_no_filters.where(F.col(c.TRACK_GROUP) == "h").count())

    def get_track_group_content_filter_table(self):
        return self.spark.createDataFrame([
            ("a", True, 300, 10_000, 5_000, 0, 0, 0, 0),
            ("b", True, 300, 100, 20, 0, 0, 0, 0),  # low stream counts
            ("c", True, 6000, 10_000, 5_000, 0, 0, 0, 0),  # high duration
            ("d", True, 15, 10_000, 5_000, 0, 0, 0, 0),  # low duration
            ("e", True, 300, 10_000, 5_000, 1, 0, 0, 0),
            ("f", True, 300, 10_000, 5_000, 0, 1, 0, 0),
            ("g", True, 300, 10_000, 5_000, 0, 0, 1, 0),
            ("h", True, 300, 10_000, 5_000, 0, 0, 0, 1),
        ], [c.TRACK_GROUP,
            c.AVAILABLE,
            c.DURATION,
            c.STREAM_COUNT,
            c.STREAMERS_COUNT,
            c.CHILDREN,
            c.HOLIDAY,
            c.AMBIENT,
            c.NON_MUSIC])
