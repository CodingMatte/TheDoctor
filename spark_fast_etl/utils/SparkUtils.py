from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class SparkUtils:

    def __init__(self, app_name: str = "DummySparkJob", master: str = "local[2]"):
        self.spark = self._create_spark_session(app_name, master)

    @staticmethod
    def _create_spark_session(self, app_name: str, master: str) -> SparkSession:
        """
        Private method for creating a Spark Session.
        """
        return (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )

    def load_local_json(self, path: str) -> DataFrame:
        return self.spark.read.json(path)

    def load_local_parquet(self, path: str) -> DataFrame:
        return self.spark.read.parquet(path)

    def close_spark_session(self):
        if self.spark is not None:
            print("Closing Spark Session...")
            self.spark.stop()
            print("Spark Session closed.")
        else:
            print("No active Spark Session to close.")

    def get_spark_session(self):
        return self.spark
