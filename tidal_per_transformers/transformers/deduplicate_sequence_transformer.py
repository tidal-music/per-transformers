from tidal_per_transformers.transformers.loggable_transformer import LoggableTransformer
import pyspark.sql.types as T
import pyspark.sql.functions as F


class DuplicateSequenceTransformer(LoggableTransformer):
    """ Remove duplicate items from a sequence """
    def __init__(self, column: str, id_field: str, schema: T.ArrayType):
        super().__init__()
        self.column = column
        self.id_field = id_field
        self.schema = schema

    def _transform(self, dataset):
        @F.udf(returnType=self.schema)
        def duplicate_sequence(sequence, id_field):
            res = []
            seen = set()
            for item in sequence:
                if item[id_field] not in seen:
                    res.append(item)
                seen.add(item[id_field])
            return res

        return dataset.withColumn(self.column, duplicate_sequence(self.column, F.lit(self.id_field)))
