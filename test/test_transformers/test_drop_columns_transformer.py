from transformers import DropColumnsTransformer
from pyspark_test import PySparkTest
import transformers.utils.constants as c


class DropColumnsTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, 10, 100),
            (1, 10, 200),
            (1, 10, 300),
            (1, 11, 100),
            (1, 12, 900)
        ], [c.USER, c.ITEM, c.SCORE])

        output = DropColumnsTransformer([c.USER]).transform(dataset)
        output2 = DropColumnsTransformer([c.USER, c.ITEM]).transform(dataset)

        self.assertEqual({c.ITEM, c.SCORE}, set(output.columns))
        self.assertEqual({c.SCORE}, set(output2.columns))

    def test_non_list_argument_raise_error(self):
        # check error is raised when columns is passed as string (not as list)
        with self.assertRaises(AssertionError):
            DropColumnsTransformer(c.USER)
