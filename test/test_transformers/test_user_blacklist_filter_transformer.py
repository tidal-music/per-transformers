import tidal_per_transformers.transformers.utils.constants as c
from pyspark_test import PySparkTest
from tidal_per_transformers.transformers.user_blacklist_filter_transformer import UserBlacklistFilterTransformer, \
    ARTIFACT_ID


class UserBlacklistFilterTransformerTest(PySparkTest):

    def test_transformer(self):
        blacklist = self.spark.createDataFrame([
            ("0", "ARTIST", "0",   1543400078036),
            ("0", "ARTIST", "1",   1543400078016),
            ("0", "TRACK",  "10",  1543400032006),
            ("1", "TRACK",  "11",  1543400048036),
            ("1", "VIDEO",  "100", 1543400458011)
        ], [c.USER_ID, c.ARTIFACT_TYPE, ARTIFACT_ID, "created"])

        compound_mapping = self.spark.createDataFrame([
            (10, 4),
            (11, 4),
        ], [c.ARTIST_ID, c.ARTIST_COMPOUND_ID])

        user_tracks = self.spark.createDataFrame([
            (0, 1, 8),
            (0, 2, 9),
            (0, 3, 10),
            (0, 4, 11),
            (1, 0, 11),
            (1, 0, 12),
            (1, 0, 13)
        ], [c.USER_ID, c.ARTIST_ID, c.TRACK_GROUP])

        output = UserBlacklistFilterTransformer(blacklist,
                                                compound_mapping,
                                                filter_track=True,
                                                filter_artist=False).transform(user_tracks)
        self.assertEqual(set(user_tracks.columns), set(output.columns))
        self.assertEqual(5, output.count())

        output = UserBlacklistFilterTransformer(blacklist,
                                                compound_mapping,
                                                filter_track=True,
                                                filter_artist=True).transform(user_tracks)
        self.assertEqual(set(user_tracks.columns), set(output.columns))
        self.assertEqual(4, output.count())

        user_videos = self.spark.createDataFrame([
            (0, 0, 110),  # Filtered due to artistId
            (0, 1, 109),  # Filtered due to artistId
            (1, 2, 100),  # Filtered due to videoId
            (1, 3, 101),  # Only video that passes filter
        ], [c.USER_ID, c.ARTIST_ID, c.VIDEO_ID])

        output = UserBlacklistFilterTransformer(blacklist,
                                                compound_mapping,
                                                filter_track=False,
                                                filter_video=True).transform(user_videos)
        self.assertEqual(set(user_videos.columns), set(output.columns))
        self.assertEqual(1, output.count())

    def test_artist_blacklisting(self):
        blacklist = self.spark.createDataFrame([
            ("0", "ARTIST", "0", 1543400078036),
            ("0", "ARTIST", "1", 1543400078016),
            ("1", "ARTIST", "4", 1543400078036),
            ("1", "ARTIST", "6", 1543400078016)
        ], [c.USER_ID, c.ARTIFACT_TYPE, ARTIFACT_ID, "created"])

        compound_table = self.spark.createDataFrame([
            (10, 4),
            (11, 4),
            (12, 6),
            (13, 6),
            (14, 7)
        ], [c.ARTIST_ID, c.ARTIST_COMPOUND_ID])

        seed_tracks = self.spark.createDataFrame([
            (0, 0, 10),
            (0, 1, 11),
            (0, 9, 12),
            (1, 10, 13),
            (1, 12, 14)
        ], [c.USER_ID, c.ARTIST_ID, c.TRACK_GROUP])

        output = UserBlacklistFilterTransformer(blacklist, compound_table).filter_blacklisted_artists(seed_tracks,
                                                                                                      blacklist)

        self.assertEqual(1, output.count())
        self.assertEqual(seed_tracks.columns, output.columns)
