from pyspark.sql.types import Row

from tidal_per_transformers.transformers import DropDuplicatesTransformer
from pyspark_test import PySparkTest
import tidal_per_transformers.transformers.utils.constants as c


class DropDuplicatesTransformerTest(PySparkTest):

    def test_transform(self):
        df = self.spark.createDataFrame([
            (1, "a", 1),
            (1, "a", 2),
            (2, "b", 1),
        ], [c.ID, c.NAME, c.COUNT])

        self.assertEqual([Row(id=1, name='a', count=1),
                          Row(id=2, name='b', count=1)],
                         DropDuplicatesTransformer([c.ID, c.NAME]).transform(df).collect())

