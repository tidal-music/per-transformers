import transformers.utils.constants as c
from transformers import BlazingTextInputFormatTransformer
from pyspark_test import PySparkTest


class BlazingTextInputFormatTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, ["a", "b", "c"]),
            (2, ["a", "c", "d", "e"]),
            (3, ["a", "b", "c", "f", "g"]),
        ], [c.ID, c.TRACKS])

        res = BlazingTextInputFormatTransformer(c.TRACKS).transform(dataset)

        self.assertEqual(3, res.count())
        self.assertEqual("a b c", res.collect()[0][c.TRACKS])

        res_alias = BlazingTextInputFormatTransformer(c.TRACKS, alias=c.ITEMS).transform(dataset)

        self.assertEqual(3, res_alias.count())
        self.assertEqual("a b c f g", res_alias.collect()[-1][c.ITEMS])
