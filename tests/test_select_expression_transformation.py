import pytest
from pyspark.sql import SparkSession
import pyspark

from spark_fast_etl.transformation.transformation import (
    SelectExpressionTransformationConfig,
    SelectExpressionTransformation,
)
from tests.utils.SparkUtils import SparkUtils


def test_select_expression_transformation():
    spark_utils = SparkUtils()
    dummy_df = spark_utils.load_local_json(
        spark_utils.spark, "test_files/dummy_df_1.json"
    )
    sel_expr_config = SelectExpressionTransformationConfig("awesome config")
    sel_expr_trans = SelectExpressionTransformation()

    sel_expr_trans.transform(dummy_df, sel_expr_config).show()

    # TODO finish checks
    assert 1 == 1
