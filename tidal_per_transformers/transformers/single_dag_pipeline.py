from typing import List

from pyspark.ml import Transformer


class SingleDAGPipeline:
    """This class creates a pipeline based on Transformers class
    The main issue with pyspark.ml.pipeline is that each stage creates its own DAG, which does not
    utilize spark new features like AQE and can lead to performance issues.
    """
    def __init__(self, stages: List[Transformer]):
        """
        :param stages: list of Transformers
        """
        self.stages = stages

    def fit(self, dataset):
        """
        :param dataset: DataFrame
        :return: PipelineModel
        """
        for stage in self.stages:
            dataset = stage.transform(dataset)
        return dataset
