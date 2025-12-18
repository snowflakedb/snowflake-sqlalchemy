#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re
from decimal import Decimal

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, inspect, select
from sqlalchemy.schema import CreateTable

from snowflake.sqlalchemy import DECFLOAT, snowdialect
from snowflake.sqlalchemy.parser.custom_type_parser import parse_type

from .util import random_string


def _normalize_ddl(ddl: str) -> str:
    """Normalize DDL string by removing extra whitespace and newlines."""
    return re.sub(r"\s+", " ", ddl).strip()


class TestDECFLOATType:
    """Tests for DECFLOAT type support."""

    def test_decfloat_type(self):
        """Test DECFLOAT type instantiation."""
        decfloat_type = DECFLOAT()
        assert decfloat_type is not None

    def test_decfloat_visit_name(self):
        """Test DECFLOAT has correct visit name."""
        decfloat_type = DECFLOAT()
        assert decfloat_type.__visit_name__ == "DECFLOAT"


class TestDECFLOATCompilation:
    """Tests for DECFLOAT DDL compilation."""

    def test_compile_decfloat(self):
        """Test DDL compilation with DECFLOAT (precision is fixed at 38)."""
        metadata = MetaData()
        test_table = Table(
            "test_decfloat",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", DECFLOAT()),
        )

        ddl = str(CreateTable(test_table).compile(dialect=snowdialect.dialect()))
        expected = (
            "CREATE TABLE test_decfloat ( "
            "id INTEGER NOT NULL AUTOINCREMENT, "
            "value DECFLOAT, "
            "PRIMARY KEY (id) )"
        )
        assert _normalize_ddl(ddl) == expected


class TestDECFLOATReflection:
    """Tests for DECFLOAT type reflection/parsing."""

    def test_parse_decfloat(self):
        """Test parsing DECFLOAT type string."""
        result = parse_type("DECFLOAT")
        assert isinstance(result, DECFLOAT)

    def test_parse_decfloat_ignores_precision(self):
        """Test that parsing DECFLOAT ignores any precision parameter.

        Snowflake DECFLOAT has fixed precision of 38 - custom precision is not supported.
        """
        result = parse_type("DECFLOAT(20)")
        assert isinstance(result, DECFLOAT)


class TestDECFLOATStringConversion:
    """Tests for DECFLOAT string conversion."""

    def test_decfloat_str_conversion(self):
        """Test that DECFLOAT can be converted to string."""
        assert str(DECFLOAT()) == "DECFLOAT"


class TestDECFLOATIntegration:
    """Integration tests for DECFLOAT type against real Snowflake database."""

    def test_create_table_with_decfloat(self, engine_testaccount):
        """Test creating a table with DECFLOAT column."""
        metadata = MetaData()
        table_name = "test_decfloat_" + random_string(5)
        test_table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", DECFLOAT()),
        )
        metadata.create_all(engine_testaccount)
        try:
            assert test_table is not None
        finally:
            test_table.drop(engine_testaccount)

    def test_insert_and_select_decfloat_values(self, engine_testaccount):
        """Test inserting and selecting DECFLOAT values."""
        metadata = MetaData()
        table_name = "test_decfloat_" + random_string(5)
        test_table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", DECFLOAT()),
        )
        metadata.create_all(engine_testaccount)
        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    # Insert regular decimal values
                    ins = test_table.insert().values(id=1, value=Decimal("123.456"))
                    conn.execute(ins)

                    ins = test_table.insert().values(
                        id=2, value=Decimal("12345678901234567890123456789.123456789")
                    )
                    conn.execute(ins)

                    # Select and verify
                    results = conn.execute(select(test_table)).fetchall()
                    assert len(results) == 2
                    assert results[0][1] == Decimal("123.456")
        finally:
            test_table.drop(engine_testaccount)

    def test_decfloat_does_not_support_special_values(self, engine_testaccount):
        """Test that DECFLOAT does NOT support special values (inf, -inf, NaN).

        Unlike FLOAT, DECFLOAT does not support special values.
        This test explicitly documents this limitation.
        """
        from sqlalchemy.exc import ProgrammingError

        table_name = "test_decfloat_" + random_string(5)
        with engine_testaccount.connect() as conn:
            with conn.begin():
                conn.exec_driver_sql(
                    f"CREATE TEMP TABLE {table_name} (id INTEGER, value DECFLOAT)"
                )

                # Verify that 'inf' is not supported - should raise an error
                with pytest.raises(ProgrammingError) as exc_info:
                    conn.exec_driver_sql(
                        f"INSERT INTO {table_name} SELECT 1, 'inf'::DECFLOAT"
                    )
                assert "not recognized" in str(exc_info.value).lower()

    def test_reflect_decfloat_column(self, engine_testaccount):
        """Test reflecting a table with DECFLOAT column."""
        table_name = "test_decfloat_" + random_string(5)
        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(
                        f"CREATE TABLE {table_name} (id INTEGER, value DECFLOAT)"
                    )

            # Reflect the table (must be outside of CREATE transaction for visibility)
            inspecter = inspect(engine_testaccount)
            columns = inspecter.get_columns(table_name)

            # Verify DECFLOAT column is reflected
            assert len(columns) == 2
            assert columns[1]["name"].lower() == "value"
            assert isinstance(columns[1]["type"], DECFLOAT)
        finally:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")
