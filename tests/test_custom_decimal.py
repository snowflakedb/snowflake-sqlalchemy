#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from decimal import Decimal

import pytest

from snowflake.sqlalchemy.custom_types import _CUSTOM_DECIMAL
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


@pytest.fixture(scope="function")
def process():
    """Yield a helper that applies a type's result processor, accounting for
    SQLAlchemy returning ``None`` (pass-through) when ``asdecimal=True`` and the
    driver already yields ``Decimal`` values."""

    def _process(decimal_type, value):
        processor = decimal_type.result_processor(SnowflakeDialect(), None)
        return processor(value) if processor is not None else value

    yield _process


def test_asdecimal_defaults_to_true():
    assert _CUSTOM_DECIMAL().asdecimal is True


def test_asdecimal_false_is_honored():
    assert _CUSTOM_DECIMAL(asdecimal=False).asdecimal is False


def test_precision_and_scale_preserved():
    decimal_type = _CUSTOM_DECIMAL(precision=10, scale=2, asdecimal=False)
    assert decimal_type.precision == 10
    assert decimal_type.scale == 2
    assert decimal_type.asdecimal is False


def test_result_is_float_when_asdecimal_false(process):
    result = process(_CUSTOM_DECIMAL(asdecimal=False), Decimal("123.45"))
    assert isinstance(result, float)
    assert result == 123.45


def test_result_is_decimal_by_default(process):
    result = process(_CUSTOM_DECIMAL(), Decimal("42.00"))
    assert isinstance(result, Decimal)
    assert result == Decimal("42.00")


def test_none_is_passed_through_when_asdecimal_false(process):
    assert process(_CUSTOM_DECIMAL(asdecimal=False), None) is None
