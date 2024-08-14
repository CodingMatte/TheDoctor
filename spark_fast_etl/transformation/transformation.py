from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from spark_fast_etl.config.costants import Constants


class TransformationConfig:
    pass


class CommonTransformationConfig(TransformationConfig):
    def __init__(self, param: str):
        self.param = param


B = TypeVar("B", bound=TransformationConfig)


class Transformation(ABC, Generic[B]):
    @abstractmethod
    def transform(self, df: DataFrame, config: B) -> DataFrame:
        pass

    @classmethod
    def get_transformation(
        cls, transformation_name: str
    ) -> Transformation[CommonTransformationConfig]:
        match transformation_name:
            case Constants.SELECT_EXPRESSION:
                return SelectExpressionTransformation()
            case _:
                raise ValueError(f"Unknown transformation name: {transformation_name}")


class SelectExpressionTransformation(Transformation[CommonTransformationConfig]):
    def transform(self, df: DataFrame, config: CommonTransformationConfig) -> DataFrame:
        return df.withColumn("select_expr", lit(config.param))
