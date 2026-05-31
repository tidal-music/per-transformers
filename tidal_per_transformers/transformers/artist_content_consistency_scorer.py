from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType

from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer


class ArtistContentConsistencyScorer(LoggableTransformer):
    """Scores how likely a track-artist pairing is correct based on metadata consistency.

    Computes a weighted consistency score (0.0 to 1.0) by comparing track-level metadata
    against pre-computed artist-level aggregates across four signals:
    - Copyright match (weight 0.35)
    - ISRC prefix match (weight 0.25)
    - Provider match (weight 0.25)
    - Duration normality (weight 0.15)
    """

    COPYRIGHT_WEIGHT = 0.35
    ISRC_PREFIX_WEIGHT = 0.25
    PROVIDER_WEIGHT = 0.25
    DURATION_WEIGHT = 0.15

    def __init__(self, artist_metadata: DataFrame):
        """Initialise the scorer with artist-level metadata.

        :param artist_metadata: DataFrame with columns artistId, modalCopyright,
            isrcPrefixes, meanDuration, stdDuration, primaryProvider
        """
        super().__init__()
        self.artist_metadata = artist_metadata

    def _transform(self, df: DataFrame) -> DataFrame:
        # Broadcast join artist metadata onto track data
        joined = df.join(
            F.broadcast(self.artist_metadata),
            on="artistId",
            how="left",
        )

        # Copyright match: 1.0 if track copyright == modal copyright, else 0.0
        copyright_score = F.when(
            F.col("copyright").isNull() | F.col("modalCopyright").isNull(), 0.0
        ).when(
            F.col("copyright") == F.col("modalCopyright"), 1.0
        ).otherwise(0.0).cast(FloatType())

        # ISRC prefix match: 1.0 if first 5 chars of ISRC in artist's prefix set, else 0.0
        isrc_prefix = F.substring(F.col("isrc"), 1, 5)
        isrc_score = F.when(
            F.col("isrc").isNull() | F.col("isrcPrefixes").isNull(), 0.0
        ).when(
            F.array_contains(F.col("isrcPrefixes"), isrc_prefix), 1.0
        ).otherwise(0.0).cast(FloatType())

        # Provider match: 1.0 if track provider == primary provider, else 0.0
        provider_score = F.when(
            F.col("provider").isNull() | F.col("primaryProvider").isNull(), 0.0
        ).when(
            F.col("provider") == F.col("primaryProvider"), 1.0
        ).otherwise(0.0).cast(FloatType())

        # Duration normality: 1.0 within 1 std dev, linear decay to 0.0 at 3 std devs
        z_score = F.abs(
            (F.col("duration").cast("float") - F.col("meanDuration")) / F.col("stdDuration")
        )
        duration_score = F.when(
            F.col("duration").isNull()
            | F.col("meanDuration").isNull()
            | F.col("stdDuration").isNull()
            | (F.col("stdDuration") == 0.0),
            0.0,
        ).when(
            z_score <= 1.0, 1.0
        ).when(
            z_score >= 3.0, 0.0
        ).otherwise(
            # Linear decay: (3 - z) / (3 - 1) = (3 - z) / 2
            (F.lit(3.0) - z_score) / F.lit(2.0)
        ).cast(FloatType())

        # Weighted sum
        consistency_score = (
            F.lit(self.COPYRIGHT_WEIGHT) * copyright_score
            + F.lit(self.ISRC_PREFIX_WEIGHT) * isrc_score
            + F.lit(self.PROVIDER_WEIGHT) * provider_score
            + F.lit(self.DURATION_WEIGHT) * duration_score
        ).cast(FloatType())

        # Select original columns plus new score, drop joined metadata columns
        original_columns = df.columns
        result = joined.withColumn("consistencyScore", consistency_score)

        return result.select(*original_columns, "consistencyScore")
