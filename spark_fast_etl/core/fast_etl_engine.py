from spark_fast_etl.config.engine_config import EngineConfig
from pyspark.sql import SparkSession


class FastEtlEngine:

    def __init__(self, engine_config: EngineConfig):
        print("BRUM BRUM... Engine Activated")
        print(engine_config.aws_key)

        self.config = engine_config
