import pytest

from spark_fast_etl.config.engine_config_reader import EngineConfigReader
from spark_fast_etl.core.fast_etl_engine import FastEtlEngine
from spark_fast_etl.spark_fast_etl import SparkFastEtl
from spark_fast_etl.utils.SparkUtils import SparkUtils


def test_read_source_df():
    spark_fast_etl = SparkFastEtl()
    imported_config = spark_fast_etl.import_etl_config("test_files/etl_config_5.yaml")
    engine_config = EngineConfigReader(imported_config).initialize_engine_content()
    spark_utils = SparkUtils(app_name="FastEtlEngine", master="local[2]")
    spark_session = spark_utils.get_spark_session()
    awesome_engine = FastEtlEngine(engine_config, spark_session)

    dummy_df = awesome_engine.read_source_df()
    dummy_df.show()

    assert dummy_df.count() == 2

    spark_utils.close_spark_session()


def test_write_df():
    spark_fast_etl = SparkFastEtl()
    imported_config = spark_fast_etl.import_etl_config("test_files/etl_config_5.yaml")
    engine_config = EngineConfigReader(imported_config).initialize_engine_content()
    spark_utils = SparkUtils(app_name="FastEtlEngine", master="local[2]")
    spark_session = spark_utils.get_spark_session()
    awesome_engine = FastEtlEngine(engine_config, spark_session)

    dummy_df = awesome_engine.read_source_df()
    dummy_df.show()
    awesome_engine.write_df(dummy_df)

    written_df = spark_utils.load_local_parquet(engine_config.local_destination)
    assert written_df.count() == 2

    spark_utils.close_spark_session()
