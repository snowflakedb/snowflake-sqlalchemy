#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import warnings

import pytest
import sqlalchemy as sa
from sqlalchemy import Integer
from sqlalchemy.sql import column, table
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
    custom_types,
)
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from .util import compile_sql, ischema_names_baseline, sa20_only, sa21_only


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


# ---------------------------------------------------------------------------
# SQLAlchemy 2.1 operator classes and ``timedelta`` literals.
# ---------------------------------------------------------------------------

_SF = SnowflakeDialect()
_ops_t = table("t", column("x", Integer), column("d", sa.Date))

_CUSTOM_TYPES = {
    "VARIANT": custom_types.VARIANT,
    "OBJECT": custom_types.OBJECT,
    "ARRAY": custom_types.ARRAY,
    "MAP": lambda: custom_types.MAP(sa.String, Integer),
    "GEOGRAPHY": custom_types.GEOGRAPHY,
    "GEOMETRY": custom_types.GEOMETRY,
    "VECTOR": lambda: custom_types.VECTOR("INT", 3),
    "DECFLOAT": custom_types.DECFLOAT,
    "TIMESTAMP_TZ": custom_types.TIMESTAMP_TZ,
    "TIMESTAMP_LTZ": custom_types.TIMESTAMP_LTZ,
    "TIMESTAMP_NTZ": custom_types.TIMESTAMP_NTZ,
}

# Operators Snowflake accepts (verified live) that must not warn on 2.1.
_TYPE_SPECIFIC_OPS = [
    ("VARIANT", lambda c: c["k"]),
    ("VARIANT", lambda c: c.contains("a")),
    ("VARIANT", lambda c: c.in_(["a", "b"])),
    ("OBJECT", lambda c: c["k"]),
    ("MAP", lambda c: c["k"]),
    ("ARRAY", lambda c: c[0]),
    ("DECFLOAT", lambda c: c + 1),
    ("DECFLOAT", lambda c: c > 1),
    ("TIMESTAMP_TZ", lambda c: c > datetime.datetime(2024, 1, 1)),
    ("TIMESTAMP_NTZ", lambda c: c.between("2024-01-01", "2024-12-31")),
    ("TIMESTAMP_LTZ", lambda c: c.is_(None)),
    ("GEOGRAPHY", lambda c: c.is_not(None)),
    ("GEOMETRY", lambda c: c.is_(None)),
    ("VECTOR", lambda c: c == [1, 2, 3]),
]


class TestOperatorClasses:
    """SQLAlchemy 2.1 validates operators against ``operator_classes``."""

    @pytest.mark.parametrize("type_name", sorted(_CUSTOM_TYPES))
    def test_custom_types_emit_no_operator_class_warning(self, type_name):
        col = column("c", _CUSTOM_TYPES[type_name]())
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            compile_sql(sa.select(_ops_t).where(col == "a"), _SF)

    @pytest.mark.parametrize("type_name, op", _TYPE_SPECIFIC_OPS)
    def test_type_specific_operators_emit_no_warning(self, type_name, op):
        col = column("c", _CUSTOM_TYPES[type_name]())
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            compile_sql(sa.select(op(col)), _SF)

    @sa21_only
    def test_every_snowflake_type_declares_operator_classes(self):
        """Guard: a new SnowflakeType subclass must set operator_classes."""

        def subclasses(cls):
            for sub in cls.__subclasses__():
                yield sub
                yield from subclasses(sub)

        missing = sorted(
            cls.__name__
            for cls in subclasses(custom_types.SnowflakeType)
            if cls.operator_classes is sa.types.OperatorClass.UNSPECIFIED
        )
        assert missing == []

    @sa21_only
    def test_vector_math_warns(self):
        """Snowflake has no VECTOR arithmetic, so ``+`` is outside its classes."""
        with pytest.warns(sa.exc.SADeprecationWarning, match="does not include"):
            column("c", custom_types.VECTOR("INT", 3)) + column(
                "d", custom_types.VECTOR("INT", 3)
            )

    @sa20_only
    def test_json_contains_sa20(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            sql = compile_sql(column("j", sa.JSON()).contains("a"), _SF)
        assert sql == "j LIKE '%%' || %(j_1)s || '%%'"

    @sa21_only
    def test_json_contains_sa21(self):
        """Not dialect-specific: SQLAlchemy 2.1 warns on JSON ``contains``."""
        with pytest.warns(sa.exc.SADeprecationWarning, match="contains_op"):
            sql = compile_sql(column("j", sa.JSON()).contains("a"), _SF)
        assert sql == "j LIKE '%%' || %(j_1)s || '%%'"


class TestIntervalLiteral:
    """``timedelta`` literals render as Snowflake INTERVAL constants (used by
    ``literal_binds`` and SQLAlchemy 2.1 ``FrameClause`` bounds)."""

    @pytest.mark.parametrize(
        "delta, rendered",
        [
            (datetime.timedelta(days=7), "INTERVAL '7 DAYS'"),
            (datetime.timedelta(hours=1), "INTERVAL '3600 SECONDS'"),
            (
                datetime.timedelta(days=1, seconds=5, microseconds=7),
                "INTERVAL '1 DAYS, 5 SECONDS, 7 MICROSECONDS'",
            ),
            (datetime.timedelta(0), "INTERVAL '0 SECONDS'"),
        ],
    )
    def test_date_plus_timedelta_literal(self, delta, rendered):
        stmt = sa.select(_ops_t.c.d + delta)
        assert compile_sql(stmt, _SF, literal_binds=True) == (
            f"SELECT t.d + {rendered} AS anon_1 FROM t"
        )

    def test_bound_timedelta_is_sent_as_epoch_datetime(self):
        """Only literal rendering is Snowflake-specific; a bound ``timedelta``
        keeps SQLAlchemy's non-native Interval behaviour (epoch offset)."""
        process = sa.Interval().dialect_impl(_SF).bind_processor(_SF)
        assert process(datetime.timedelta(days=1, seconds=5)) == datetime.datetime(
            1970, 1, 2, 0, 0, 5
        )

    def test_negative_timedelta_is_rejected(self):
        stmt = sa.select(_ops_t.c.d + datetime.timedelta(days=-1))
        with pytest.raises(sa.exc.CompileError) as ei:
            compile_sql(stmt, _SF, literal_binds=True)
        # SQLAlchemy wraps the literal processor error; the cause explains it.
        assert "negative interval" in str(ei.value.__cause__)
