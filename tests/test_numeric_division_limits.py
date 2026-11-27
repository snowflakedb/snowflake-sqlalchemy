#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Document Snowflake's fixed-point division limit.

Snowflake derives the result type of ``NUMBER / NUMBER`` from the *declared*
precision and scale of the operands, not from their values. Up to server
10.38, wide-scale operands give an intermediate type above the 38-digit maximum
and the statement is rejected at compile time::

    001814 (22000): SQL compilation error:
    Invalid intermediate datatype: NUMBER(61,40).

This is a server-side rule, independent of SQLAlchemy and of its version.
SQLAlchemy 2.1 (and 2.0.55+) added ``TrueDivTest.test_truediv_divisor`` cases
that hit it (``numeric_many_decimals``, ``numeric_tiny_divisor``).

Server 10.39 accepts these divisions. While test deployments run both 10.38
and 10.39, the rejection tests below and the upstream cases in
``sqlalchemy_test_suite/test_suite.py`` are non-strict xfails, each limited to
the exception the other server version produces. TODO(SNOW-4199118): once every
test deployment is on 10.39+, delete the two ``*Rejected`` classes and the
upstream-case xfail.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from _pytest.outcomes import Failed
from sqlalchemy import Numeric, cast, func, literal_column, select
from sqlalchemy.exc import ProgrammingError

COMPILATION_ERROR_CODE = 1814

# Snowflake 10.39+ accepts these divisions, so pytest.raises fails with "DID NOT
# RAISE" (``Failed``); any other error still fails the test. Non-strict: on 10.38
# the division is rejected as asserted and the test reports XPASS.
accepted_since_10_39 = pytest.mark.xfail(
    raises=Failed,
    strict=False,
    reason="Snowflake 10.39+ accepts wide-scale NUMBER division; 10.38 rejects it",
)

MANY_DECIMALS = ("5.02797279921331664759", "0.71828182845904523537")
# NUMBER(p,p) only holds values below 1, so the scale == precision cases need
# operands that fit; otherwise 10.39+ fails the CAST itself (100046, out of
# representable range) instead of exercising the division.
SUB_ONE = ("0.50279727992133166475", "0.71828182845904523537")
TINY_DIVISOR = ("0.00000000000049382716", "0.00000000000012345679")


def _assert_intermediate_type_error(exc_info, intermediate_type: str) -> None:
    orig = exc_info.value.orig
    assert orig.errno == COMPILATION_ERROR_CODE
    assert f"Invalid intermediate datatype: {intermediate_type}" in str(orig)


@pytest.mark.parametrize(
    "left, right, type_sql, intermediate_type",
    [
        pytest.param(*MANY_DECIMALS, "NUMBER(22,20)", "NUMBER(61,40)", id="22_20"),
        pytest.param(*SUB_ONE, "NUMBER(20,20)", "NUMBER(61,40)", id="20_20"),
        pytest.param(*SUB_ONE, "NUMBER(19,19)", "NUMBER(59,39)", id="19_19"),
        pytest.param(*TINY_DIVISOR, "NUMBER(38,20)", "NUMBER(61,40)", id="38_20"),
    ],
)
@accepted_since_10_39
class TestRawFixedPointDivisionRejected:
    """Plain SQL, no SQLAlchemy expression involved."""

    def test_division_is_rejected(
        self, engine_testaccount, left, right, type_sql, intermediate_type
    ):
        sql = f"SELECT CAST({left} AS {type_sql}) / CAST({right} AS {type_sql})"
        with engine_testaccount.connect() as conn:
            with pytest.raises(ProgrammingError) as exc_info:
                conn.exec_driver_sql(sql)
        _assert_intermediate_type_error(exc_info, intermediate_type)

    def test_outer_cast_does_not_help(
        self, engine_testaccount, left, right, type_sql, intermediate_type
    ):
        """The division is typed before the outer CAST, so it still fails."""
        sql = (
            f"SELECT CAST(CAST({left} AS {type_sql}) / CAST({right} AS {type_sql}) "
            "AS NUMBER(38,12))"
        )
        with engine_testaccount.connect() as conn:
            with pytest.raises(ProgrammingError) as exc_info:
                conn.exec_driver_sql(sql)
        _assert_intermediate_type_error(exc_info, intermediate_type)


@pytest.mark.parametrize(
    "left, right, type_, intermediate_type",
    [
        pytest.param(*MANY_DECIMALS, Numeric(22, 20), "NUMBER(61,40)", id="many"),
        pytest.param(*TINY_DIVISOR, Numeric(38, 20), "NUMBER(61,40)", id="tiny"),
    ],
)
@accepted_since_10_39
class TestSqlalchemyNumericDivisionRejected:
    """The SQL SQLAlchemy emits for ``Numeric / Numeric`` hits the same limit."""

    def test_typed_divisor(
        self, engine_testaccount, left, right, type_, intermediate_type
    ):
        expr = cast(literal_column(left), type_) / cast(literal_column(right), type_)
        with engine_testaccount.connect() as conn:
            with pytest.raises(ProgrammingError) as exc_info:
                conn.execute(select(expr)).scalar()
        _assert_intermediate_type_error(exc_info, intermediate_type)

    def test_untyped_function_divisor(
        self, engine_testaccount, left, right, type_, intermediate_type
    ):
        expr = cast(literal_column(left), type_) / func.nullif(
            cast(literal_column(right), type_), 0
        )
        with engine_testaccount.connect() as conn:
            with pytest.raises(ProgrammingError) as exc_info:
                conn.execute(select(expr)).scalar()
        _assert_intermediate_type_error(exc_info, intermediate_type)


class TestWorkarounds:
    """Cases that stay valid: they bound where the limit starts."""

    def test_reduced_scale_succeeds(self, engine_testaccount):
        left, right = MANY_DECIMALS
        with engine_testaccount.connect() as conn:
            result = conn.exec_driver_sql(
                f"SELECT CAST({left} AS NUMBER(21,20)) / CAST({right} AS NUMBER(21,20))"
            ).scalar()
        assert result == Decimal(7)

    def test_wide_integer_operands_succeed(self, engine_testaccount):
        """Scale 0 keeps the intermediate type within range."""
        with engine_testaccount.connect() as conn:
            result = conn.exec_driver_sql(
                "SELECT CAST('30000000000000000000000000000000000000' AS NUMBER(38,0))"
                " / CAST('10000000000000000000000000000000000000' AS NUMBER(38,0))"
            ).scalar()
        assert result == Decimal(3)

    @pytest.mark.parametrize(
        "operands, expected",
        [(MANY_DECIMALS, Decimal(7)), (TINY_DIVISOR, Decimal(4))],
        ids=["many", "tiny"],
    )
    def test_decfloat_succeeds(self, engine_testaccount, operands, expected):
        left, right = operands
        with engine_testaccount.connect() as conn:
            result = conn.exec_driver_sql(
                f"SELECT CAST('{left}' AS DECFLOAT) / CAST('{right}' AS DECFLOAT)"
            ).scalar()
        assert Decimal(str(result)) == expected
