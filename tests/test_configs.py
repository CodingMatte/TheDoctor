import pytest

from spark_fast_etl.config.costants import Constants
from spark_fast_etl.config.engine_config import EngineConfig
from spark_fast_etl.config.engine_config_reader import EngineConfigReader
from spark_fast_etl.spark_fast_etl import SparkFastEtl


def config_starts(path_config_test_file: str) -> EngineConfig:
    spark_fast_etl = SparkFastEtl()
    imported_config = spark_fast_etl.import_etl_config(path_config_test_file)
    engine_config = EngineConfigReader(
        imported_config, "mykey", "mysecret"
    ).initialize_engine_content()

    return engine_config


def test_costants():
    constants = Constants()

    assert constants.S3_SOURCE == "s3_source_bucket"
    assert constants.S3_DESTINATION == "s3_destination_bucket"


def test_engine_config_reader():
    # aws sources test
    engine_config = config_starts("test_files/etl_config_1.yaml")

    assert engine_config.aws_key == "mykey"
    assert engine_config.s3_destination_bucket == "my-awesome-bucket"

    # aws destination test
    engine_config = config_starts("test_files/etl_config_4.yaml")

    assert engine_config.env == "local"
    assert engine_config.local_destination == "my_local_path_destination"


def test_engine_config():

    with pytest.raises(ValueError) as exc_info:
        engine_config = config_starts("test_files/etl_config_2.yaml")

    with pytest.raises(ValueError) as exc_info:
        engine_config = config_starts("test_files/etl_config_3.yaml")
