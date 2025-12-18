#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re
from decimal import Decimal

from sqlalchemy import Column, Integer, MetaData, Table, inspect, select, text
from sqlalchemy.schema import CreateTable

from snowflake.sqlalchemy import DECFLOAT, snowdialect
from snowflake.sqlalchemy.parser.custom_type_parser import parse_type

from .util import random_string


def _normalize_ddl(ddl: str) -> str:
    """Normalize DDL string by removing extra whitespace and newlines."""
    return re.sub(r"\s+", " ", ddl).strip()


class TestDECFLOATType:
    """Tests for DECFLOAT type support."""

    def test_decfloat_default_precision(self):
        """Test DECFLOAT type with default precision (38)."""
        decfloat_type = DECFLOAT()
        assert decfloat_type.precision == 38

    def test_decfloat_custom_precision(self):
        """Test DECFLOAT type with custom precision."""
        decfloat_type = DECFLOAT(precision=20)
        assert decfloat_type.precision == 20

    def test_decfloat_visit_name(self):
        """Test DECFLOAT has correct visit name."""
        decfloat_type = DECFLOAT()
        assert decfloat_type.__visit_name__ == "DECFLOAT"


class TestDECFLOATCompilation:
    """Tests for DECFLOAT DDL compilation."""

    def test_compile_decfloat_default(self):
        """Test DDL compilation with default DECFLOAT."""
        metadata = MetaData()
        test_table = Table(
            "test_decfloat_default",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", DECFLOAT()),
        )

        ddl = str(CreateTable(test_table).compile(dialect=snowdialect.dialect()))
        expected = (
            "CREATE TABLE test_decfloat_default ( "
            "id INTEGER NOT NULL AUTOINCREMENT, "
            "value DECFLOAT, "
            "PRIMARY KEY (id) )"
        )
        assert _normalize_ddl(ddl) == expected

    def test_compile_decfloat_custom_precision(self):
        """Test DDL compilation with custom precision DECFLOAT."""
        metadata = MetaData()
        test_table = Table(
            "test_decfloat_precision",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value", DECFLOAT(precision=20)),
        )

        ddl = str(CreateTable(test_table).compile(dialect=snowdialect.dialect()))
        expected = (
            "CREATE TABLE test_decfloat_precision ( "
            "id INTEGER NOT NULL AUTOINCREMENT, "
            "value DECFLOAT(20), "
            "PRIMARY KEY (id) )"
        )
        assert _normalize_ddl(ddl) == expected


class TestDECFLOATBindProcessor:
    """Tests for DECFLOAT bind processor (special value handling)."""

    def test_bind_processor_infinity(self):
        """Test bind processor handles positive infinity."""
        decfloat_type = DECFLOAT()
        processor = decfloat_type.bind_processor(None)
        assert processor(float("inf")) == "inf"

    def test_bind_processor_negative_infinity(self):
        """Test bind processor handles negative infinity."""
        decfloat_type = DECFLOAT()
        processor = decfloat_type.bind_processor(None)
        assert processor(float("-inf")) == "-inf"

    def test_bind_processor_nan(self):
        """Test bind processor handles NaN."""
        decfloat_type = DECFLOAT()
        processor = decfloat_type.bind_processor(None)
        assert processor(float("nan")) == "NaN"

    def test_bind_processor_regular_value(self):
        """Test bind processor passes through regular values."""
        decfloat_type = DECFLOAT()
        processor = decfloat_type.bind_processor(None)
        assert processor(123.456) == 123.456
        assert processor(0) == 0
        assert processor(-999.999) == -999.999

    def test_bind_processor_none(self):
        """Test bind processor handles None."""
        decfloat_type = DECFLOAT()
        processor = decfloat_type.bind_processor(None)
        assert processor(None) is None


class TestDECFLOATReflection:
    """Tests for DECFLOAT type reflection/parsing."""

    def test_parse_decfloat_no_params(self):
        """Test parsing DECFLOAT without parameters."""
        result = parse_type("DECFLOAT")
        assert isinstance(result, DECFLOAT)
        assert result.precision == 38

    def test_parse_decfloat_with_precision(self):
        """Test parsing DECFLOAT with precision."""
        result = parse_type("DECFLOAT(20)")
        assert isinstance(result, DECFLOAT)
        assert result.precision == 20

    def test_parse_decfloat_max_precision(self):
        """Test parsing DECFLOAT with max precision."""
        result = parse_type("DECFLOAT(38)")
        assert isinstance(result, DECFLOAT)
        assert result.precision == 38


class TestDECFLOATStringConversion:
    """Tests for DECFLOAT string conversion."""

    def test_decfloat_str_conversion(self):
        """Test that DECFLOAT can be converted to string."""
        assert str(DECFLOAT()) == "DECFLOAT"
        assert str(DECFLOAT(precision=20)) == "DECFLOAT(20)"


class TestDECFLOATIntegration:
    """Integration tests for DECFLOAT type against real Snowflake database."""

    def test_create_table_with_decfloat(self, engine_testaccount):
        """Test creating a table with DECFLOAT columns."""
        metadata = MetaData()
        table_name = "test_decfloat_" + random_string(5)
        test_table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("value_default", DECFLOAT()),
            Column("value_precision", DECFLOAT(precision=20)),
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

    def test_decfloat_special_values(self, engine_testaccount):
        """Test DECFLOAT special values (inf, -inf, NaN) using raw SQL."""
        table_name = "test_decfloat_" + random_string(5)
        with engine_testaccount.connect() as conn:
            with conn.begin():
                # Create table with raw SQL
                conn.exec_driver_sql(
                    f"CREATE TEMP TABLE {table_name} (id INTEGER, value DECFLOAT)"
                )

                # Insert special values using DECFLOAT literals
                conn.exec_driver_sql(
                    f"INSERT INTO {table_name} VALUES (1, 'inf'::DECFLOAT)"
                )
                conn.exec_driver_sql(
                    f"INSERT INTO {table_name} VALUES (2, '-inf'::DECFLOAT)"
                )
                conn.exec_driver_sql(
                    f"INSERT INTO {table_name} VALUES (3, 'NaN'::DECFLOAT)"
                )

                # Select and verify special values
                result = conn.execute(
                    text(f"SELECT value FROM {table_name} ORDER BY id")
                )
                rows = result.fetchall()
                assert len(rows) == 3
                # Snowflake returns these as strings
                assert str(rows[0][0]) == "Infinity"
                assert str(rows[1][0]) == "-Infinity"
                assert str(rows[2][0]) == "NaN"

    def test_reflect_decfloat_column(self, engine_testaccount):
        """Test reflecting a table with DECFLOAT column."""
        table_name = "test_decfloat_" + random_string(5)
        with engine_testaccount.connect() as conn:
            with conn.begin():
                conn.exec_driver_sql(
                    f"CREATE TEMP TABLE {table_name} (id INTEGER, value DECFLOAT)"
                )

                # Reflect the table
                inspecter = inspect(engine_testaccount)
                columns = inspecter.get_columns(table_name)

                # Verify DECFLOAT column is reflected
                assert len(columns) == 2
                assert columns[1]["name"] == "value"
                assert isinstance(columns[1]["type"], DECFLOAT)
