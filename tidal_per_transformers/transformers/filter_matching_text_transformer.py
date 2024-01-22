from pyspark.ml import Transformer


# TODO: add unit tests

class FilterMatchingTextTransformer(Transformer):
    """
    Filter out when the text is matched in the column.
    """

    def __init__(self, column_name, texts, exact_match=False, case_sensitive=False):
        """
        :param column_name: name of the column to find the text
        :param text: list of text to find
        :param exact_match: should find exact text or surround with the wildcard %
        :param case_sensitive: match with the same case or independently
        """
        super(FilterMatchingTextTransformer, self).__init__()
        self.column_name = column_name
        self.texts = texts
        self.exact_match = exact_match
        self.case_sensitive = case_sensitive

    def _transform(self, dataset):

        if not self.case_sensitive:
            self.column_name = f"lower({self.column_name})"

        where = "1=1"

        for text in self.texts:

            if text is None:
                raise Exception("Cannot use NULL value in FilterMatchingTextTransformer")

            if not self.exact_match:
                text = f"%{text}%"

            if not self.case_sensitive:
                text = f"{text.lower()}"

            cond = f" and {self.column_name} not like '{text}'"

            where = where + cond

            # need to add not null or sql will remove all nulls
        where = f"({where}) or ({self.column_name} is null)"

        return dataset.where(where)
