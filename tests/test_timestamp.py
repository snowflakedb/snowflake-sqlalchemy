#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from datetime import datetime

import pandas as pd
import pytest
import pytz
from sqlalchemy import Column, DateTime, Integer, MetaData, Table, inspect, text
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import select
from sqlalchemy.types import TIMESTAMP

from snowflake.sqlalchemy import TIMESTAMP_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ, snowdialect
from snowflake.sqlalchemy.custom_types import _CUSTOM_DateTime
from tests.util import compile_type, normalize_ddl, random_string

PST_TZ = "America/Los_Angeles"
JST_TZ = "Asia/Tokyo"


class TestUnitDatetimeAndTimestampWithTimezone:
    """Unit tests for issue #199: DateTime/TIMESTAMP timezone handling."""

    def test_datetime_without_timezone_compiles_to_datetime(self):
        assert compile_type(DateTime(timezone=False)) == "datetime"

    def test_datetime_with_timezone_compiles_to_timestamp_tz(self):
        assert compile_type(DateTime(timezone=True)) == "TIMESTAMP_TZ"

    def test_timestamp_without_timezone_compiles_to_timestamp(self):
        assert compile_type(TIMESTAMP(timezone=False)) == "TIMESTAMP"

    def test_timestamp_with_timezone_compiles_to_timestamp_tz(self):
        assert compile_type(TIMESTAMP(timezone=True)) == "TIMESTAMP_TZ"

    def test_custom_datetime_preserves_timezone_flag(self):
        custom_datetime = DateTime(timezone=True).adapt(_CUSTOM_DateTime)
        assert getattr(custom_datetime, "timezone", None) is True

    def test_custom_datetime_without_timezone_flag(self):
        custom_datetime = DateTime(timezone=False).adapt(_CUSTOM_DateTime)
        assert getattr(custom_datetime, "timezone", None) is False

    def test_create_table_datetime_timezone_true_ddl(self):
        metadata = MetaData()
        table = Table(
            "test_timestamps_with_timezones",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("with_timezone", DateTime(timezone=True)),
            Column("without_timezone", DateTime(timezone=False)),
        )
        ddl = normalize_ddl(
            str(CreateTable(table).compile(dialect=snowdialect.dialect()))
        )
        assert "with_timezone TIMESTAMP_TZ" in ddl
        assert "without_timezone datetime" in ddl.lower()

    def test_create_table_timestamp_timezone_true_ddl(self):
        metadata = MetaData()
        table = Table(
            "test_timestamps_with_timezones",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("with_timezone", TIMESTAMP(timezone=True)),
            Column("without_timezone", TIMESTAMP(timezone=False)),
        )
        ddl = normalize_ddl(
            str(CreateTable(table).compile(dialect=snowdialect.dialect()))
        )
        assert "with_timezone TIMESTAMP_TZ" in ddl
        assert "without_timezone TIMESTAMP," in ddl

    def test_create_table_explicit_snowflake_types_ddl(self):
        metadata = MetaData()
        table = Table(
            "test_timestamps_with_explicit_timezones",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("ntz", TIMESTAMP_NTZ()),
            Column("tz", TIMESTAMP_TZ()),
            Column("ltz", TIMESTAMP_LTZ()),
        )
        ddl = normalize_ddl(
            str(CreateTable(table).compile(dialect=snowdialect.dialect()))
        )
        assert "ntz TIMESTAMP_NTZ" in ddl
        assert "tz TIMESTAMP_TZ" in ddl
        assert "ltz TIMESTAMP_LTZ" in ddl


class TestIntegrationDatetimeAndTimestampWithTimezone:
    """Integration tests for issue #199 against a real Snowflake account."""

    def test_datetime_with_timezone(self, engine_testaccount):
        table_name = "test_datetime_with_timezone" + random_string(8)
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("with_timezone", DateTime(timezone=True)),
            Column("without_timezone", DateTime(timezone=False)),
        )
        metadata.create_all(engine_testaccount)

        try:
            insp = inspect(engine_testaccount)
            cols = {c["name"]: c for c in insp.get_columns(table_name)}

            assert cols["with_timezone"]["type"].__class__.__name__ == "TIMESTAMP_TZ"
            assert (
                cols["without_timezone"]["type"].__class__.__name__ == "TIMESTAMP_NTZ"
            )
        finally:
            table.drop(engine_testaccount)

    def test_timestamp_with_timezone(self, engine_testaccount):
        table_name = "test_timestamp_with_timezone" + random_string(8)
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("with_timezone", TIMESTAMP(timezone=True)),
            Column("without_timezone", TIMESTAMP(timezone=False)),
        )
        metadata.create_all(engine_testaccount)

        try:
            insp = inspect(engine_testaccount)
            cols = {c["name"]: c for c in insp.get_columns(table_name)}

            assert cols["with_timezone"]["type"].__class__.__name__ == "TIMESTAMP_TZ"
            assert (
                cols["without_timezone"]["type"].__class__.__name__ == "TIMESTAMP_NTZ"
            )
        finally:
            table.drop(engine_testaccount)

    def test_explicit_timestamps_with_timezones(self, engine_testaccount):
        table_name = "test_explicit_timestamps_with_timezones" + random_string(8)
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("col_ntz", TIMESTAMP_NTZ()),
            Column("col_tz", TIMESTAMP_TZ()),
            Column("col_ltz", TIMESTAMP_LTZ()),
        )
        metadata.create_all(engine_testaccount)

        try:
            insp = inspect(engine_testaccount)
            cols = {c["name"]: c for c in insp.get_columns(table_name)}

            assert cols["col_ntz"]["type"].__class__.__name__ == "TIMESTAMP_NTZ"
            assert cols["col_tz"]["type"].__class__.__name__ == "TIMESTAMP_TZ"
            assert cols["col_ltz"]["type"].__class__.__name__ == "TIMESTAMP_LTZ"
        finally:
            table.drop(engine_testaccount)

    def test_get_ddl_confirms_timezone_for_datetime_and_timestamp(
        self, engine_testaccount
    ):
        table_name = (
            "test_get_ddl_confirms_tz_for_datetime_and_timestamp" + random_string(8)
        )
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("datetime_with_timezone", DateTime(timezone=True)),
            Column("timestamp_with_timezone", TIMESTAMP(timezone=True)),
        )
        metadata.create_all(engine_testaccount)

        try:
            with engine_testaccount.connect() as conn:
                result = conn.execute(text(f"SELECT GET_DDL('TABLE', '{table_name}')"))
                ddl_text = normalize_ddl(str(result.scalar())).lower()

            assert "datetime_with_timezone timestamp_tz" in ddl_text
            assert "timestamp_with_timezone timestamp_tz" in ddl_text
        finally:
            table.drop(engine_testaccount)


@pytest.mark.pandas
class TestPandasTimezoneReproduction:
    """Reproduction of the original issue #199 scenario: pandas to_sql()
    with timezone-aware DataFrame columns creates TIMESTAMP_TZ columns."""

    def test_pandas_to_sql_with_timezone_aware_timestamps_uses_timestamp_tz(
        self, engine_testaccount
    ):
        """When pandas encounters a timezone-aware datetime column it infers
        DateTime(timezone=True). The dialect must emit TIMESTAMP_TZ."""

        table_name = (
            "test_pandas_to_sql_with_timezone_aware_timestamps_uses_timestamp_tz"
            + random_string(8)
        )
        df = pd.DataFrame(
            {
                "id": [1, 2],
                "with_timezone": pd.to_datetime(
                    ["2024-01-01 12:00:00", "2024-06-15 18:30:00"]
                ).tz_localize("UTC"),
            }
        )
        try:
            with engine_testaccount.connect() as conn:
                df.to_sql(table_name, conn, index=False, if_exists="replace")

            insp = inspect(engine_testaccount)
            cols = {c["name"]: c for c in insp.get_columns(table_name)}
            assert cols["with_timezone"]["type"].__class__.__name__ == "TIMESTAMP_TZ"
        finally:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))


def test_create_table_timestamp_datatypes(engine_testaccount):
    """
    Create table including timestamp data types
    """
    metadata = MetaData()
    table_name = "test_timestamp0"
    test_timestamp = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("tsntz", TIMESTAMP_NTZ),
        Column("tsltz", TIMESTAMP_LTZ),
        Column("tstz", TIMESTAMP_TZ),
    )
    metadata.create_all(engine_testaccount)
    try:
        assert test_timestamp is not None
    finally:
        test_timestamp.drop(engine_testaccount)


def test_inspect_timestamp_datatypes(engine_testaccount):
    """
    Create table including timestamp data types
    """
    metadata = MetaData()
    table_name = "test_timestamp0"
    test_timestamp = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("tsntz", TIMESTAMP_NTZ),
        Column("tsltz", TIMESTAMP_LTZ),
        Column("tstz", TIMESTAMP_TZ),
    )
    metadata.create_all(engine_testaccount)
    try:
        current_utctime = datetime.utcnow()
        current_localtime = pytz.utc.localize(current_utctime, is_dst=False).astimezone(
            pytz.timezone(PST_TZ)
        )
        current_localtime_without_tz = datetime.now()
        current_localtime_with_other_tz = pytz.utc.localize(
            current_localtime_without_tz, is_dst=False
        ).astimezone(pytz.timezone(JST_TZ))

        ins = test_timestamp.insert().values(
            id=1,
            tsntz=current_utctime,
            tsltz=current_localtime,
            tstz=current_localtime_with_other_tz,
        )
        with engine_testaccount.connect() as conn:
            with conn.begin():
                results = conn.execute(ins)
                results.close()

                s = select(test_timestamp)
                results = conn.execute(s)
                rows = results.fetchone()
                results.close()
                assert rows[0] == 1
                assert rows[1] == current_utctime
                assert rows[2] == current_localtime
                assert rows[3] == current_localtime_with_other_tz
    finally:
        test_timestamp.drop(engine_testaccount)
