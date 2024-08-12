from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


class SparkUtils:

    def __init__(self, app_name: str = "DummySparkJob", master: str = "local[2]"):
        self.spark = self.create_spark_session(app_name, master)

    def create_spark_session(self, app_name: str, master: str) -> SparkSession:
        """
        Method for create a Spark Session
        """
        spark = (
            SparkSession.builder.appName(app_name)
            .master(master)
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        return spark

    def load_local_json(self, spark: SparkSession, path: str) -> DataFrame:
        return spark.read.json(path)
