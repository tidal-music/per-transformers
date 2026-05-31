from pyspark.sql.types import (
    ArrayType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from test.pyspark_test import PySparkTest
from tidal_per_transformers.transformers.artist_content_consistency_scorer import (
    ArtistContentConsistencyScorer,
)


class TestArtistContentConsistencyScorer(PySparkTest):

    TRACK_SCHEMA = StructType([
        StructField("artistId", StringType(), True),
        StructField("trackGroup", StringType(), True),
        StructField("copyright", StringType(), True),
        StructField("isrc", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("provider", StringType(), True),
    ])

    ARTIST_METADATA_SCHEMA = StructType([
        StructField("artistId", StringType(), True),
        StructField("modalCopyright", StringType(), True),
        StructField("isrcPrefixes", ArrayType(StringType()), True),
        StructField("meanDuration", FloatType(), True),
        StructField("stdDuration", FloatType(), True),
        StructField("primaryProvider", StringType(), True),
    ])

    def _create_artist_metadata(self):
        data = [
            (
                "artist_carlos_santana",
                "Sony Music Entertainment",
                ["USSM1", "USSM2"],
                240.0,
                30.0,
                "Sony Music",
            ),
        ]
        return self.spark.createDataFrame(data, self.ARTIST_METADATA_SCHEMA)

    def test_legitimate_track_scores_high(self):
        """All signals match - score should be ~1.0."""
        tracks = self.spark.createDataFrame(
            [(
                "artist_carlos_santana",
                "track_001",
                "Sony Music Entertainment",
                "USSM1234567",
                235,
                "Sony Music",
            )],
            self.TRACK_SCHEMA,
        )
        artist_metadata = self._create_artist_metadata()

        transformer = ArtistContentConsistencyScorer(artist_metadata=artist_metadata)
        result = transformer.transform(tracks)

        row = result.collect()[0]
        self.assertAlmostEqual(row["consistencyScore"], 1.0, places=2)

    def test_wrong_artist_scores_low(self):
        """Santana rapper on Carlos Santana - copyright, ISRC, provider all mismatch."""
        tracks = self.spark.createDataFrame(
            [(
                "artist_carlos_santana",
                "track_002",
                "Jive Records",
                "USJI50012345",
                220,
                "Zomba Recording",
            )],
            self.TRACK_SCHEMA,
        )
        artist_metadata = self._create_artist_metadata()

        transformer = ArtistContentConsistencyScorer(artist_metadata=artist_metadata)
        result = transformer.transform(tracks)

        row = result.collect()[0]
        # Only duration normality contributes (220 is within 1 std of 240)
        # Expected: 0.15 * 1.0 = 0.15
        self.assertAlmostEqual(row["consistencyScore"], 0.15, places=2)

    def test_test_upload_scores_very_low(self):
        """Kokubo 'Do Re Mi test' - copyright mismatch, ISRC mismatch, very short duration."""
        tracks = self.spark.createDataFrame(
            [(
                "artist_carlos_santana",
                "track_003",
                "Independent Upload",
                "GBXXX000001",
                15,
                "TuneCore",
            )],
            self.TRACK_SCHEMA,
        )
        artist_metadata = self._create_artist_metadata()

        transformer = ArtistContentConsistencyScorer(artist_metadata=artist_metadata)
        result = transformer.transform(tracks)

        row = result.collect()[0]
        # Duration z-score: |15 - 240| / 30 = 7.5 -> beyond 3 std devs -> 0.0
        # All signals 0.0 -> score 0.0
        self.assertAlmostEqual(row["consistencyScore"], 0.0, places=2)

    def test_legitimate_reissue_scores_moderate(self):
        """Copyright different but ISRC and provider match - reissue scenario."""
        tracks = self.spark.createDataFrame(
            [(
                "artist_carlos_santana",
                "track_004",
                "Legacy Recordings",
                "USSM1999888",
                250,
                "Sony Music",
            )],
            self.TRACK_SCHEMA,
        )
        artist_metadata = self._create_artist_metadata()

        transformer = ArtistContentConsistencyScorer(artist_metadata=artist_metadata)
        result = transformer.transform(tracks)

        row = result.collect()[0]
        # Copyright: 0.0 (mismatch) -> 0.35 * 0.0 = 0.0
        # ISRC: 1.0 (USSM1 in prefixes) -> 0.25 * 1.0 = 0.25
        # Provider: 1.0 (match) -> 0.25 * 1.0 = 0.25
        # Duration: z = |250-240|/30 = 0.33 -> within 1 std -> 1.0 -> 0.15 * 1.0 = 0.15
        # Total: 0.65
        self.assertAlmostEqual(row["consistencyScore"], 0.65, places=2)

    def test_null_handling_does_not_crash(self):
        """Missing copyright or ISRC should score 0.0 for that signal, not crash."""
        tracks = self.spark.createDataFrame(
            [(
                "artist_carlos_santana",
                "track_005",
                None,
                None,
                230,
                "Sony Music",
            )],
            self.TRACK_SCHEMA,
        )
        artist_metadata = self._create_artist_metadata()

        transformer = ArtistContentConsistencyScorer(artist_metadata=artist_metadata)
        result = transformer.transform(tracks)

        row = result.collect()[0]
        # Copyright: 0.0 (null) -> 0.35 * 0.0 = 0.0
        # ISRC: 0.0 (null) -> 0.25 * 0.0 = 0.0
        # Provider: 1.0 (match) -> 0.25 * 1.0 = 0.25
        # Duration: z = |230-240|/30 = 0.33 -> within 1 std -> 1.0 -> 0.15 * 1.0 = 0.15
        # Total: 0.40
        self.assertAlmostEqual(row["consistencyScore"], 0.40, places=2)
        self.assertEqual(result.count(), 1)
