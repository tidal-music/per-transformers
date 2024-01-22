from pyspark_test import PySparkTest
import tidal_per_transformers.transformers.utils.constants as c
from tidal_per_transformers.transformers.filter_dead_and_inactive_artists_transformer import \
    FilterDeadAndInactiveArtistsTransformer


class FilterDeadAndInactiveArtistsTransformerTest(PySparkTest):

    def test_transformer(self):
        dataset = self.sc.createDataFrame([
            (79386403, "A", 1, "1e7aed86737846c9f"),
            (79386403, "B", 2, "1e7aed86737846c9g"),
            (79386403, "C", 3, "1e7aed86737846c9g"),
            (79386403, "D", 4, "1e7aed86737846c9g"),
            (79386403, "E", 5, "1e7aed86737846c9g"),
            (79386403, "F", 6, "1e7aed86737846c9g"),
            (79386403, "G", 7, "1e7aed86737846c9g"),
            (79417725, "H", 7, "1e7aadb86e3df939f"),
            (79516638, "I", 8, "1e7aae6e09524719f"),
            (79386479, "J", 9, "1e7aed872188d309f"),
            (79386403, "K", 11, "1e7aed86737846c9f"),
            (79386403, "L", 12, "1e7aed86737846c9f")
        ], [c.RELEASE_DATE, c.TITLE, c.ARTIST_ID, c.MASTER_BUNDLE_ID])

        metadata = self.sc.createDataFrame([
            ("1", "Active Years", "1958-??-??_2001-??-??"),
            ("2", "Active Years", "1998-??-??_2001-??-??"),
            ("3", "Active Years", "2008-??-??_2011-??-??"),
            ("4", "Active Years", "2006-??-??_2017-??-??"),
            ("5", "Active Years", "2007-??-??_2013-??-??"),
            ("6", "Active Years", "2016-??-??_2038-??-??"),
            ("7", "Death Date", "1998-10-04"),
            ("8", "Death Date", "2006-06-23"),
            ("9", "Death Date", "2012-??-??"),
            ("10", "Death Date", "2017-05-16"),
            ("11", "Death Date", "2039-03-11")
        ], [c.ARTIFACT_ID, c.TYPE, c.VALUE])

        def test_with_mocks():

            output = FilterDeadAndInactiveArtistsTransformer(metadata).transform(dataset)

            assert (output.count() == 3)
            artists = output.select(c.ARTIST_ID).orderBy(c.ARTIST_ID).collect()
            assert 6 == artists[0][c.ARTIST_ID]
            assert 11 == artists[1][c.ARTIST_ID]
            assert 12 == artists[2][c.ARTIST_ID]

        test_with_mocks()


