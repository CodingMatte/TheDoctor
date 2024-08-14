import yaml

from spark_fast_etl.config.engine_config_reader import EngineConfigReader

from spark_fast_etl.core.fast_etl_engine import FastEtlEngine


class SparkFastEtl:

    def import_etl_config(self, path_to_config: str) -> dict:
        with open(path_to_config, "r") as file:
            etl_config_content = yaml.safe_load(file)

        return etl_config_content

    # def initialize_engine(self, config_content: dict) -> Engine_Config:


if __name__ == "__main__":
    spark_fast_etl = SparkFastEtl()

    # TODO: add it from parameter
    config_content = spark_fast_etl.import_etl_config(
        "tests/test_files/etl_config_1.yaml"
    )

    # TODO: add keys from
    engine_config = EngineConfigReader(
        config_content, "my_awesome_key", "my_awesome_secret"
    ).initialize_engine_content()
    awesome_engine = FastEtlEngine(engine_config)

    awesome_engine.execute()
