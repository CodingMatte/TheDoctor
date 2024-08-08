import pytest

from spark_fast_etl.transformation.transformation import (
    SelectExpressionTransformationConfig,
    SelectExpressionTransformation,
)


def test_select_expression_transformation():
    sel_expr_config = SelectExpressionTransformationConfig("awesome config")
    sel_expr_trans = SelectExpressionTransformation()

    assert sel_expr_trans.transform(sel_expr_config) == "awesome config"
