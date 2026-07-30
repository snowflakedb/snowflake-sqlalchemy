#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal

import pytest
from sqlalchemy.types import INTEGER, VARCHAR

import snowflake.sqlalchemy
from snowflake.sqlalchemy import (
    ARRAY,
    DECFLOAT,
    GEOGRAPHY,
    GEOMETRY,
    MAP,
    OBJECT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
    VECTOR,
)
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from .util import ischema_names_baseline


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names

    for k, _ in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None


def test_type_baseline():
    assert set(SnowflakeDialect.ischema_names.keys()) == set(
        ischema_names_baseline.keys()
    )
    for k, v in SnowflakeDialect.ischema_names.items():
        assert issubclass(v, ischema_names_baseline[k])


@pytest.mark.parametrize(
    "type_instance, expected_python_type",
    [
        (VARIANT(), dict),
        (OBJECT(), dict),
        (MAP(VARCHAR(), INTEGER()), dict),
        (ARRAY(), list),
        (VECTOR("FLOAT", 3), list),
        (TIMESTAMP_TZ(), datetime.datetime),
        (TIMESTAMP_LTZ(), datetime.datetime),
        (TIMESTAMP_NTZ(), datetime.datetime),
        (GEOGRAPHY(), str),
        (GEOMETRY(), str),
        (DECFLOAT(), decimal.Decimal),
    ],
)
def test_python_type(type_instance, expected_python_type):
    """Snowflake custom types expose a ``python_type`` (SNOW-1866493)."""
    assert type_instance.python_type is expected_python_type
