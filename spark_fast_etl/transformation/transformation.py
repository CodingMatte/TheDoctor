from abc import ABC, abstractmethod
from typing import Generic, TypeVar


class TransformationConfig:
    pass


class SelectExpressionTransformationConfig(TransformationConfig):
    def __init__(self, param: str):
        self.param = param


B = TypeVar("B", bound=TransformationConfig)


class Transformation(ABC, Generic[B]):
    @abstractmethod
    def transform(self, config: B) -> None:
        pass


class SelectExpressionTransformation(
    Transformation[SelectExpressionTransformationConfig]
):
    def transform(self, config: SelectExpressionTransformationConfig):
        print(config.param)
        return config.param
