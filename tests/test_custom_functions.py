#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from sqlalchemy.sql import func

from snowflake.sqlalchemy import snowdialect


def test_flatten_function():
    flatten_func = func.flatten()

    compiled = flatten_func.compile(dialect=snowdialect.dialect())
    assert str(compiled) == "FLATTEN()"


def test_flatten_with_params():
    flat = func.flatten("[1, 2]", order=True)
    res = flat.compile(dialect=snowdialect.dialect())

    assert str(res) == "FLATTEN(%(FLATTEN_1)s)"
