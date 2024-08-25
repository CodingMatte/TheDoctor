from spark_fast_etl.config.costants import Constants
from spark_fast_etl.config.engine_config import EngineConfig
from pyspark.sql import DataFrame, SparkSession

from spark_fast_etl.utils.SparkUtils import SparkUtils


class FastEtlEngine:

    def __init__(self, engine_config: EngineConfig, spark: SparkSession):
        print("BRUM BRUM... Engine Activated")
        print(engine_config.aws_key)

        self.config = engine_config
        self.spark = spark

    def read_source_df(self) -> DataFrame:
        match self.config.env:
            case Constants.LOCAL:
                path_to_read = self.config.local_source
            case Constants.DEV:
                # TODO: test from s3
                path_to_read = self.config.s3_source_bucket + self.config.s3_source_path
            case Constants.PRE:
                path_to_read = self.config.s3_source_bucket + self.config.s3_source_path
            case Constants.PRO:
                path_to_read = self.config.s3_source_bucket + self.config.s3_source_path
            case _:
                raise ValueError(f"Unknown environment: {self.config.env}")

        match self.config.source_format:
            case Constants.JSON:
                # TODO: add spark schema
                return self.spark.read.option("mergeSchema", "true").json(path_to_read)
            case Constants.CSV:
                return (
                    self.spark.read.option("mergeSchema", "true")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv(path_to_read)
                )
            case Constants.PARQUET:
                return self.spark.read.option("mergeSchema", "true").parquet(
                    path_to_read
                )
            case _:
                raise ValueError(
                    f"Unknown source format name: {self.config.source_format}"
                )

    def write_df(self, df: DataFrame):
        match self.config.env:
            case Constants.LOCAL:
                output_path = self.config.local_destination
            case Constants.DEV:
                # TODO: test from s3
                output_path = (
                    self.config.s3_destination_bucket
                    + self.config.s3_destination_prefix
                )
            case Constants.PRE:
                output_path = (
                    self.config.s3_destination_bucket
                    + self.config.s3_destination_prefix
                )
            case Constants.PRO:
                output_path = (
                    self.config.s3_destination_bucket
                    + self.config.s3_destination_prefix
                )
            case _:
                raise ValueError(f"Unknown environment: {self.config.env}")

        match self.config.destination_format:
            case Constants.JSON:
                # TODO switch from overwrite to config merge type
                df.write.mode("overwrite").json(output_path)
            case Constants.CSV:
                df.write.mode("overwrite").csv(output_path, header=True)
            case Constants.PARQUET:
                df.write.mode("overwrite").parquet(output_path)
            case _:
                raise ValueError(
                    f"Unknown destination format name: {self.config.destination_format}"
                )

    def execute(self):
        source_df = self.read_source_df()
        source_df.show()

        return None
