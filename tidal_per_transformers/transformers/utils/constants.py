from enum import Enum


# ----- Enums -----
class Artifact(Enum):
    TRACK = "track"
    TRACK_GROUP = "trackGroup"
    ARTIST = "artist"
    TRACKBUNDLE = "trackBundle"
    MASTER_BUNDLE_ID = "masterBundle"
    MASTERBUNDLE = "masterBundle"
    PLAYLIST = "playlist"
    VIDEO = "video"


class PlaylistCategory(Enum):
    DOLBY = "DOLBY"
    IMMERSIVE = "IMMERSIVE"
    RISING = "RISING"
    PRODUCED_BY = "PRODUCED_BY"
    FLAGSHIPS = "FLAGSHIPS"
    CLASSICS = "CLASSICS"
    BEST_OF = "BEST_OF"
    ESSENTIALS = "ESSENTIALS"
    TRACKTRACES = "TRACKTRACES"
    HITS = "HITS"
    ONE_O_ONE = "101"


MAIN_ARTIST_FILTER = "filter(artists, x -> x.main == 'true')"

RN = "rn"
ITEM = "item"
ITEMS = "items"
ITEMS_TO_KEEP = "itemsToKeep"
USER = "user"
SCORE = "score"
TRACK_GROUP = "trackGroup"
COUNT = "count"
HIDDEN_PARAMS = "_params"
DELETED = "deleted"
AVAILABLE = "available"
AVAILABLE_COUNTRY_CODES = "AvailableCountryCodes"
SIZE = "size"

CONTRIBUTOR_ID = "contributorId"
CONTRIBUTOR_TYPE = "contributorType"

# ----- Dataset Constants -----
# IDs
ID = "id"
UID = "uid"
USER_ID = "userId"
ARTIST_ID = "artistId"
MAIN_ARTIST_ID = "mainArtistId"
PLAYLIST_ID = "playlistId"
PRODUCT_ID = "productId"
SOURCE_ID = "sourceId"
ALBUM_ID = "albumId"
MASTER_BUNDLE_ID = "masterBundleId"
TRACK_BUNDLE_ID = "trackBundleId"
MIX_ID = "mixId"
ARTIFACT_ID = "artifact_id"
VIDEO_ID = "videoId"

DATA_FORMAT = "dataFormat"
TRACKS = "tracks"
ARTISTS = "artists"
VOICE = "voice"
PRODUCT_TYPE = "productType"
ACTUAL_PRODUCT_ID = "actualProductId"
SOURCE_TYPE = "sourceType"
START_TIMESTAMP = "startTimestamp"
END_TIMESTAMP = "endTimestamp"
STREAM_DURATION = "streamDuration"
SERVER_TS = "serverTs"
COUNTRY_CODES = "countryCodes"
COUNTRY_CODE = "countryCode"
TITLE = "title"

AUDIO_QUALITY = "audioQuality"
GENRE = "genre"
SUM = "sum"
TYPE = "type"
VALUE = "value"
VARIABLE = "variable"
DT = 'dt'
VARIANT = "variant"

# ----- Rovi dataset constants -----
ROVI = "ROVI"
SOURCE = "source"
ROVI_SOURCE_ID = "source_id"
ROVI_ARTIFACT_TYPE_ID = "artifact_type_id"
ROVI_ARTIFACT_ID = "artifact_id"
ROVI_METADATA_ID = "metadata_id"
ROVI_ARTIST = "ARTIST"
METADATA_TYPE_ID = "metadata_type_id"
PLAYBACKLOG = "playbacklog"
USER_STATE = "userstate"

ROVI_ACTIVE_YEARS = "Active Years"
ACTIVE_YEARS = "activeYears"
ROVI_COUNTRY_OF_ORIGIN = "Country of Origin"
COUNTRY_OF_ORIGINS = "countryOfOrigins"
ROVI_INSTRUMENTATION_ENSEMBLE = "Instrumentation-Ensemble"
INSTRUMENTATION_ENSEMBLES = "instrumentationEnsembles"
ROVI_NAME_ROLE = "Name Role"
NAME_ROLES = "nameRoles"
ROVI_TONE = "Tone"
TONES = "tones"
ROVI_THEME = "Theme"
THEMES = "themes"
ROVI_ORIGINAL_LANGUAGE = "Original language"
ORIGINAL_LANGUAGE = "originalLanguage"
ROVI_GROUP_MEMBER_CONTRIBUTOR_DATES = "Group Member/Contributor Dates"
GROUP_MEMBER_CONTRIBUTOR_DATES = "groupMemberContributorDates"
ROVI_GENDER = "Gender"
GENDERS = "genders"
ROVI_MUSICAL_GENRE = "Musical Genre"
MUSICAL_GENRES = "musicalGenres"
ROVI_MAIN_CREDIT_INSTRUMENT = "Main Credit/Instrument"
MAIN_CREDIT_INSTRUMENTS = "mainCreditInstruments"
ROVI_PERIOD = "Period"
PERIODS = "periods"
ROVI_SHORT_NOTE = "Short Note"
SHORT_NOTES = "shortNotes"
ROVI_DECADE = "Decade"
DECADES = "decades"
ROVI_AREA_OF_OPERATION = "Area Of Operation"
AREA_OF_OPERATION = "areaOfOperation"
ROVI_ENSEMBLE_TYPE = "Ensemble Type"
ENSEMBLE_TYPE = "ensembleType"
ROVI_BIRTH_DATE = "Birth Date"
BIRTH_DATES = "birthDates"
ROVI_BIRTH_NAME = "Birth Name"
BIRTH_NAMES = "birthNames"
ROVI_DEATH_DATE = "Death Date"
DEATH_DATES = "deathDates"

ASSOCIATION_NAME = "association_name"

START = "start"
END = "end"

ALIASES = "aliases"
PICTURE = "picture"

STEPPED_LENGTH = "steppedLength"
PARTITION = "partition"
GROUP = "group"
INDEX = "index"


class MetadataType(Enum):
    BIOGRAPHY = 1
    ASSOCIATION = 2
    ARTIFACT_IMAGE = 3
    REVIEW = 4
    LINK = 5
    TOP_LISTS = 7
    ATTRIBUTE = 8
    PRODUCT_IMAGE = 9
    GENRE = 10
    CREDIT = 11


class AssociationName(Enum):
    ARTIST_HAS_PERFORMED_SONGS_BY = "ARTIST_HAS_PERFORMED_SONGS_BY"
    PRIMARY_NAME = "PRIMARY_NAME"
    ARTIST_COMPOSER_NAME = "ARTIST_COMPOSER_NAME"
    CONDUCTED = "CONDUCTED"
    RELATIVE_OF = "RELATIVE_OF"
    ARTIST_SIMILAR_TO = "ARTIST_SIMILAR_TO"
    SIBLING_OF = "SIBLING_OF"
    CHILD_OF = "CHILD_OF"
    STUDIED_WITH = "STUDIED_WITH"
    ALSO_PERFORMED_AS = "ALSO_PERFORMED_AS"
    ARTIST_PERFORMED_WORKS_BY = "ARTIST_PERFORMED_WORKS_BY"
    COLLABORATOR_WITH = "COLLABORATOR_WITH"
    FOUNDER = "FOUNDER"
    FOLLOWED = "FOLLOWED"
    INFLUENCED = "INFLUENCED"
    PARENT_OF = "PARENT_OF"
    ARTIST_WORKS_PERFORMED_BY = "ARTIST_WORKS_PERFORMED_BY"
    MEMBER_OF = "MEMBER_OF"
    IS_ASSOCIATED_WITH = "IS_ASSOCIATED_WITH"
    MARRIED_TO = "MARRIED_TO"
    LABEL_GROUP_FOR = "LABEL_GROUP_FOR"
    COMPOSER_SIMILAR_TO = "COMPOSER_SIMILAR_TO"
    GRANDPARENT_OF = "GRANDPARENT_OF"
    GRANDCHILD_OF = "GRANDCHILD_OF"
    DISTRIBUTOR = "DISTRIBUTOR"
    CONTRIBUTOR_FOR = "CONTRIBUTOR_FOR"


# ----- Column Names -----
DATE_ADDED = "dateAdded"
TRACK_ID = "trackId"
FAVORITE = "favorite"
PROFILE_ID = "profileId"
LAST_UPDATED = "lastUpdated"
REGISTERED_DATE = "registeredDate"
LAST_DATE_PROCESSED = "lastDateProcessed"
POPULARITY = "popularity"
POPULARITY_WW = "popularityWW"
POPULARITY_WEIGHTED = "popularityWeighted"
POPULARITY_ALL_TIME = "popularityAllTime"
ACTUAL = "actual"
VARIOUS_ARTIST_ALBUM = "variousArtistAlbum"
ALBUM = "album"
ALBUM_TYPE = "albumType"
ARTIST_IMAGE = "artistImage"
RELEASE_DATE = "releaseDate"
NUM_FEATURING_ALBUMS = "numFeaturingAlbums"
MIN_RELEASE_DATE = "minReleaseDate"
LAST_STREAMED_DATE = "lastStreamedDate"
RECENT = "recent"
RELEVANT = "relevant"

# ---- Misc Constants -----
MAIN_ARTISTS = "mainArtists"
MAIN_ARTIST = "mainArtist"
FEATURING_ARTISTS = "featuringArtists"
NAME = "name"
AGE = "age"
ARTIST_NAME = "artistName"
ARTIST_AGE = "artistAge"
MAIN = "main"
NON_MUSIC = "non_music"
STREAM_COUNT = "streamCount"
STREAMERS_COUNT = "streamersCount"
DURATION = "duration"
LOG_ENTRY_DATE = "logEntryDate"
COMPLETION = "completion"
DATETIME = "datetime"
CHILDREN = "children"
AMBIENT = "ambient"
HOLIDAY = "holiday"
MATCHES = "matches"
ARTIST_COMPOUND_ID = "artistCompoundId"
RESOLVED_ARTIST_ID = "resolvedArtistId"
PRIORITY = "priority"
EXISTING = "existing"
UPDATED = "updated"
DATE = "date"
RESULT = 'result'
EXCEPTION_CONFIG = 'expectation_config'
DAY = "day"
MONTH = "month"
YEAR = "year"
MASTER_ALBUM = "masterAlbum"
DATABRICKS_MLFLOW_URI = "databricks"

# ----- Feature store -----

TRANSACTIONS = "transactions"
STREAMS = "STREAMS"
USER_TRACK = "user_track"
USER_ARTIST = "user_artist"
USER_ARTIST_STREAMS = "user_artist_streams"
USER_ARTISTS_DISCOVERY_OBSERVED = "UserArtistsDiscoveryObserved"
USER_ARTISTS_FAVOURITES = 'UserArtistsFavourite'
USER_ARTISTS_HISTORY = 'UserArtistsHistory'
USER_ARTISTS_PLAYLISTS = 'UserArtistsPlayLists'
USER_TRACKS_DISCOVERY_OBSERVED = 'UserTracksDiscoveryObserved'
USER_TRACKS_FAVOURITES = 'UserTracksFavourite'
USER_TRACKS_HISTORY = 'UserTracksHistory'
USER_TRACKS_PLAYLIST = 'UserTracksPlayLists'
NUM_MAIN_ARTISTS = "numMainArtists"
ARTIFACT_TYPE = "artifactType"
COMPOUND_ID = "compoundId"
RANK = "rank"

# ----- Celeste Metadata -----
METADATA_ID = "metadata_id"
METADATA_TYPE = "metadataType"
METADATA_VALUE = "value"
ASSOCIATION_ROLE_ID = "association_role_id"
ASSOCIATION_ARTIFACT_TYPE_ID = "association_artifact_type_id"
ASSOCIATION_ARTIFACT_ID = "association_artifact_id"
ASSOCIATION_ID = "associationId"
WEIGHT = "weight"

# ----- FS -----
METADATA = 'metadata'
USERS = 'users'

ARTIFACT_TYPE_ID = {
    "TRACK": 1,
    "ARTIST": 2,
    "TRACKBUNDLE": 3,
    "VIDEO": 4,
    "PLAYLIST": 5,
    "MASTERBUNDLE": 6,
    "TRACKGROUP": 7
}

# ----- Write modes -----
OVERWRITE = 'overwrite'
UPSERT = 'upsert'
APPEND = 'append'
ERROR = 'error'
IGNORE = 'ignore'

# ----- Options -----
OVERWRITE_SCHEMA = 'overwriteSchema'

# ----- formats -----
DELTA = "delta"
PARQUET = "parquet"
