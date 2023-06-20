import pyspark.sql.types as T
import tidal_per_transformers.transformers.utils.constants as c


ARTISTS_SCHEMA = T.ArrayType(T.StructType([
    T.StructField(c.ALIASES, T.ArrayType(T.StringType())),
    T.StructField(c.ID, T.IntegerType()),
    T.StructField(c.NAME, T.StringType()),
    T.StructField(c.MAIN, T.BooleanType()),
    T.StructField(c.PICTURE, T.StringType())
]))


PLAYLIST_TRACKS_SCHEMA = T.ArrayType(T.StructType([
    T.StructField(c.INDEX, T.IntegerType()),
    T.StructField(c.PRODUCT_ID, T.IntegerType()),
    T.StructField(c.TITLE, T.StringType()),
    T.StructField(c.TRACK_GROUP, T.StringType()),
    T.StructField(c.ALBUM_ID, T.IntegerType()),
    T.StructField(c.MASTER_BUNDLE_ID, T.StringType()),
    T.StructField(c.ARTIST_ID, T.IntegerType()),
    T.StructField(c.DURATION, T.DoubleType()),
    T.StructField(c.AUDIO_QUALITY, T.StringType()),
    T.StructField(c.GENRE, T.StringType())]))
