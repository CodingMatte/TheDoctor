from spark_fast_etl.config.engine_config import EngineConfig


class FastEtlEngine:

    def __init__(self, engine_config: EngineConfig):
        print("BRUM BRUM... Engine Activated")
        print(engine_config.aws_key)
