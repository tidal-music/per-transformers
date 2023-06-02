import utils.constants as c
from transformers.clean_text_transformer import CleanTextTransformer

from pyspark_test import PySparkTest


class CleanTextTransformerTest(PySparkTest):

    def test_transform(self):
        dataset = self.spark.createDataFrame([
            (1, "Title a"),  # 1
            (2, "title a"),
            (2, "title    a"),
            (3, "Title a (Remix)"),
            (3, "Title a (Remix) ?"),
            (3, "Title, a"),
            (3, "Title. a"),
            (3, "Title. a !"),
            (4, "Title a (Live)"),
            (4, "Title a [Live]"),
            (4, "Title a / Live"),
            (4, "Title a (Remix)[Mono]"),
            (4, "Title a (Remix)[Mono][2017 Remastered]"),
            (5, "Où sont les femmes"),  # 2
            (6, "Ou sont les femmes"),
            (7, "Où sont les femmes ?"),
            (8, " سـركِ (Arabic Version)"),  # 3
            (9, "Слагам край (Bulgarian Version)"),  # 4
            (10, "Taakse jää (Finnish Version)"),  # 5
            (11, "レット・イット・ゴー～ありのままで～ (Japanese Version)"),  # 6
            (12, "다 잊어 (Korean Version)"),  # 7
            (13, "随它吧 (Mandarin Version)"),  # 8
            (14, "放開手 (Taiwanese Mandarin Version)"),  # 9
            (15, "ปล่อยมันไป (Thai Version)")  # 10
        ], [c.TRACK_GROUP, c.TITLE])

        output = (CleanTextTransformer()
                  .transform(dataset)
                  .select(c.TITLE)
                  .distinct()
                  .orderBy(c.TITLE)
                  .collect())

        assert 10 == len(output)
        assert "ousontlesfemmes" == output[0][c.TITLE]
        assert "taaksejaa" == output[1][c.TITLE]
        assert "titlea" == output[2][c.TITLE]
