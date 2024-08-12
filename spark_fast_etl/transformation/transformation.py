from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


class TransformationConfig:
    pass


class SelectExpressionTransformationConfig(TransformationConfig):
    def __init__(self, param: str):
        self.param = param


B = TypeVar("B", bound=TransformationConfig)


class Transformation(ABC, Generic[B]):
    @abstractmethod
    def transform(self, df: DataFrame, config: B) -> DataFrame:
        pass


class SelectExpressionTransformation(
    Transformation[SelectExpressionTransformationConfig]
):
    def transform(
        self, df: DataFrame, config: SelectExpressionTransformationConfig
    ) -> DataFrame:
        return df.withColumn("select_expr", lit(config.param))
