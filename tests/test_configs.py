import pytest

from spark_fast_etl.config.config import Constants

def test_costants():
    constants = Constants()

    assert constants.S3_SOURCE == "s3_source_bucket"
    assert constants.S3_DESTINATION == "s3_destination_bucket"
