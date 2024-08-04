import pytest

from spark_fast_etl.spark_fast_etl import SparkFastEtl

def test_import_etl_config():
    spark_fast_etl = SparkFastEtl()
    imported_config = spark_fast_etl.import_etl_config("test_files/etl_config_1.yaml")

    assert type(imported_config) == dict
    assert len(imported_config) == 1
    assert len(imported_config["etl_config"]) == 4
    assert imported_config["etl_config"]["input"]["source_format"] == "csv"

