#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from decimal import Decimal

from sqlalchemy import cast, func, literal, select

from snowflake.sqlalchemy import DECFLOAT


def test_select_decfloat(engine_testaccount):
    select_stmt = select(cast(literal("123.45"), DECFLOAT))

    with engine_testaccount.connect() as connection:
        result = connection.execute(select_stmt)
        value = result.scalar_one()

        assert value == Decimal("123.45")


def test_select_decfloat_sum(engine_testaccount):
    expr = cast(literal("123.45"), DECFLOAT) + cast(literal("100.55"), DECFLOAT)
    select_stmt = select(func.sum(expr))

    with engine_testaccount.connect() as connection:
        result = connection.execute(select_stmt)
        value = result.scalar_one()

        assert value == Decimal("224.00")
