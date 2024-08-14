from spark_fast_etl.config.costants import Constants
from spark_fast_etl.config.engine_config import EngineConfig
from pyspark.sql import DataFrame

from spark_fast_etl.utils.SparkUtils import SparkUtils


class FastEtlEngine:

    def __init__(self, engine_config: EngineConfig):
        print("BRUM BRUM... Engine Activated")
        print(engine_config.aws_key)

        self.config = engine_config
        self.spark = SparkUtils.create_spark_session("FastEtlEngine", "local[2]")

    def read_source_df(self) -> DataFrame:
        if self.config.local_source:
            path_to_read = self.config.local_source
        else:
            # TODO: test from s3
            path_to_read = self.config.s3_source_bucket + self.config.s3_source_path

        match self.config.local_source:
            case Constants.JSON:
                # TODO: add spark schema
                return (
                    self.spark.read.option("mergeSchema", "true")
                    .schema(None)
                    .json(path_to_read)
                )
            case Constants.CSV:
                return (
                    self.spark.read.option("mergeSchema", "true")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .schema(None)
                    .csv(path_to_read)
                )
            case Constants.PARQUET:
                return (
                    self.spark.read.option("mergeSchema", "true")
                    .schema(None)
                    .parquet(path_to_read)
                )
            case _:
                raise ValueError(
                    f"Unknown source format name: {self.config.source_format}"
                )

    def execute(self):
        source_df = self.read_source_df()

        return None
