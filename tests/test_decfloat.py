#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import decimal
import re
import sys
import warnings
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

    @pytest.fixture(autouse=True)
    def reset_decfloat_state(self):
        """Reset DECFLOAT warning flag and decimal precision before/after each test."""
        original_prec = decimal.getcontext().prec
        DECFLOAT._warned_precision = False
        yield
        decimal.getcontext().prec = original_prec
        DECFLOAT._warned_precision = False

    def test_decfloat_type(self):
        """Test DECFLOAT type instantiation."""
        decfloat_type = DECFLOAT()
        assert decfloat_type is not None

    def test_decfloat_warns_on_low_precision_context(self):
        """Test that DECFLOAT warns when decimal context precision is too low."""
        decimal.getcontext().prec = 28

        decfloat_type = DECFLOAT()
        processor = decfloat_type.result_processor(None, None)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processor(Decimal("123.456"))  # First call - should warn
            processor(Decimal("789.012"))  # Second call - should not warn again

            assert len(w) == 1, "Warning should only be emitted once"
            assert "decimal context precision (28)" in str(w[0].message)
            assert "DECFLOAT precision (38)" in str(w[0].message)

    def test_decfloat_no_warning_when_precision_sufficient(self):
        """Test that DECFLOAT does not warn when precision is >= 38."""
        decimal.getcontext().prec = 38

        decfloat_type = DECFLOAT()
        processor = decfloat_type.result_processor(None, None)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processor(Decimal("123.456"))

            assert len(w) == 0

    def test_decfloat_no_warning_when_precision_above_38(self):
        """Test that DECFLOAT does not warn when precision is above 38.

        When decimal context precision > 38, there's no risk of truncation.
        Python can hold all 38 DECFLOAT digits without loss.
        """
        decimal.getcontext().prec = 50  # Above DECFLOAT's 38

        decfloat_type = DECFLOAT()
        processor = decfloat_type.result_processor(None, None)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processor(Decimal("123.456"))

            assert len(w) == 0

    def test_decfloat_no_warning_when_dialect_has_decfloat_enabled(self):
        """Test that DECFLOAT does not warn when dialect has enable_decfloat set."""
        # Set low precision that would normally trigger warning
        decimal.getcontext().prec = 28

        # Create mock dialect with _enable_decfloat set
        class MockDialect:
            _enable_decfloat = True

        decfloat_type = DECFLOAT()
        processor = decfloat_type.result_processor(MockDialect(), None)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            processor(Decimal("123.456"))

            # No warning because dialect has DECFLOAT support enabled
            assert len(w) == 0

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

    @pytest.mark.skipif(
        sys.version_info < (3, 9),
        reason="DECFLOAT requires snowflake-connector-python >= 3.14.1",
    )
    def test_decfloat_precision_with_enable_decfloat_parameter(self, request):
        """Test that enable_decfloat dialect parameter sets decimal context.

        When enable_decfloat=True is set in the URL, the dialect should
        automatically set decimal context precision to 38 on connect.
        """
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        from tests.conftest import url_factory

        # Create engine with enable_decfloat=True
        url = url_factory(enable_decfloat=True)
        engine = create_engine(
            url,
            poolclass=NullPool,
            future=True,
            connect_args={"disable_ocsp_checks": True, "insecure_mode": True},
        )
        request.addfinalizer(engine.dispose)

        table_name = "test_decfloat_" + random_string(5)
        # Value with exactly 38 significant digits
        value_38_digits = Decimal("12345678901234567890123456789.123456789")

        with engine.connect() as conn:
            with conn.begin():
                conn.exec_driver_sql(f"CREATE TEMP TABLE {table_name} (value DECFLOAT)")
                conn.exec_driver_sql(
                    f"INSERT INTO {table_name} SELECT {value_38_digits}::DECFLOAT"
                )

                # With enable_decfloat=True, full precision should be preserved
                result = conn.exec_driver_sql(
                    f"SELECT value FROM {table_name}"
                ).fetchone()[0]
                digits = len(result.as_tuple().digits)

                assert (
                    digits == 38
                ), "enable_decfloat=True should preserve full precision"
                assert result == value_38_digits

    @pytest.mark.skipif(
        sys.version_info < (3, 9),
        reason="DECFLOAT requires snowflake-connector-python >= 3.14.1",
    )
    def test_decfloat_precision_depends_on_decimal_context(self, engine_testaccount):
        """Test that Python decimal context affects DECFLOAT precision from connector.

        The Snowflake Python connector uses Python's decimal context when
        converting DECFLOAT values to Decimal objects. Setting context.prec=38
        preserves full DECFLOAT precision; default (28) truncates.

        Users needing full 38-digit precision should set:
            decimal.getcontext().prec = 38
        """
        import decimal

        table_name = "test_decfloat_" + random_string(5)
        # Value with exactly 38 significant digits
        value_38_digits = Decimal("12345678901234567890123456789.123456789")
        original_prec = decimal.getcontext().prec

        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(
                        f"CREATE TEMP TABLE {table_name} (value DECFLOAT)"
                    )
                    conn.exec_driver_sql(
                        f"INSERT INTO {table_name} SELECT {value_38_digits}::DECFLOAT"
                    )

                    # Test with default context (28 digits) - precision is lost
                    decimal.getcontext().prec = 28
                    result_28 = conn.exec_driver_sql(
                        f"SELECT value FROM {table_name}"
                    ).fetchone()[0]
                    digits_28 = len(result_28.as_tuple().digits)

                    # Test with context=38 - full precision preserved
                    decimal.getcontext().prec = 38
                    result_38 = conn.exec_driver_sql(
                        f"SELECT value FROM {table_name}"
                    ).fetchone()[0]
                    digits_38 = len(result_38.as_tuple().digits)

                    # Document behavior
                    assert digits_28 < 38, "Default context truncates precision"
                    assert digits_38 == 38, "Context=38 preserves full precision"
                    assert result_38 == value_38_digits
        finally:
            decimal.getcontext().prec = original_prec

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
            inspector = inspect(engine_testaccount)
            assert inspector.has_table(
                table_name
            ), f"Table {table_name} was not created"

            columns = inspector.get_columns(table_name)
            value_col = next(c for c in columns if c["name"].lower() == "value")
            assert isinstance(value_col["type"], DECFLOAT)
        finally:
            test_table.drop(engine_testaccount)

    @pytest.mark.skipif(
        sys.version_info < (3, 9),
        reason="DECFLOAT requires snowflake-connector-python >= 3.14.1",
    )
    def test_insert_and_select_decfloat_values(self, engine_testaccount):
        """Test inserting and selecting DECFLOAT values.

        Note: While Snowflake DECFLOAT stores up to 38 digits, the Snowflake
        Python connector may return values with reduced precision based on
        Python's decimal context (default 28 digits).
        """
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
                    conn.execute(
                        test_table.insert().values(id=1, value=Decimal("123.456"))
                    )
                    # Use value within typical precision range
                    conn.execute(
                        test_table.insert().values(
                            id=2, value=Decimal("9999999999999999.999999999999")
                        )
                    )

                    results = conn.execute(
                        select(test_table).order_by(test_table.c.id)
                    ).fetchall()
                    assert len(results) == 2
                    assert results[0][1] == Decimal("123.456")
                    assert results[1][1] == Decimal("9999999999999999.999999999999")
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
