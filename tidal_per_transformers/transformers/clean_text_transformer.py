import unicodedata

import pandas as pd
import pyspark.sql.functions as F
from alphabet_detector import AlphabetDetector
from pyspark.ml.base import Transformer
from pyspark.sql.types import StringType

import tidal_per_transformers.transformers.utils.constants as c


@F.pandas_udf(returnType=StringType())
def strip_accents_udf(texts: pd.Series) -> pd.Series:

    def _strip_accents(text):
        if not AlphabetDetector().only_alphabet_chars(text, "LATIN"):
            return text

        try:
            # noinspection PyUnresolvedReferences
            text = unicode(text, 'utf-8')
        except NameError:  # unicode is a default on python 3
            pass

        return (unicodedata.normalize('NFD', text)
                .encode('ascii', 'ignore')
                .decode("utf-8"))

    return pd.Series([_strip_accents(text) for text in texts])


class CleanTextTransformer(Transformer):

    def __init__(self, text_col=c.TITLE, output_col=c.TITLE):
        super(CleanTextTransformer, self).__init__()
        self.text_col = text_col
        self.output_col = output_col

    def _transform(self, dataset):

        return (dataset
                .withColumn(self.output_col, F.regexp_replace(F.lower(F.col(self.text_col)), r'\([^)]*\)', ""))  # All text inside ()
                .withColumn(self.output_col, F.regexp_replace(F.col(self.output_col), r'\[[^\]]*\]', ""))  # All text inside []
                .withColumn(self.output_col, F.regexp_replace(F.col(self.output_col), r'/[^/]*$', ""))  # All text after /
                .withColumn(self.output_col, strip_accents_udf(self.output_col))
                .withColumn(self.output_col, F.trim(F.regexp_replace(F.col(self.output_col), r'[\?,.!\s]', ""))))  # Remove ?



