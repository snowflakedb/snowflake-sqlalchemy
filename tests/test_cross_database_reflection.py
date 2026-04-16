#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import MetaData, Table, inspect, select, text
from sqlalchemy.exc import NoSuchTableError

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


class TestSchemaParsingUnit:
    """Unit tests for schema parsing helpers — no Snowflake account needed."""

    def test_split_schema_by_dot_database_schema(self):
        dialect = SnowflakeDialect()
        parts = dialect.identifier_preparer._split_schema_by_dot("database.schema")
        assert len(parts) == 2
        assert str(parts[0]) == "database"
        assert str(parts[1]) == "schema"

    def test_split_schema_by_dot_quoted(self):
        dialect = SnowflakeDialect()
        parts = dialect.identifier_preparer._split_schema_by_dot('"database"."schema"')
        assert len(parts) == 2
        assert str(parts[0]) == "database"
        assert str(parts[1]) == "schema"

    def test_split_schema_by_dot_quoted_dots(self):
        dialect = SnowflakeDialect()
        parts = dialect.identifier_preparer._split_schema_by_dot('"schema.with.dots"')
        assert len(parts) == 1
        assert str(parts[0]) == "schema.with.dots"

    def test_split_schema_by_dot_mixed(self):
        dialect = SnowflakeDialect()
        parts = dialect.identifier_preparer._split_schema_by_dot(
            'test_db."schema.with.dots"'
        )
        assert len(parts) == 2
        assert str(parts[0]) == "test_db"
        assert str(parts[1]) == "schema.with.dots"

    def test_get_full_schema_name_cross_database(self):
        """Cross-database schema should use both parts, not prepend current_database."""
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            result = dialect._get_full_schema_name(mock_conn, "test_db.my_schema")
            assert "test_db" in result
            assert "my_schema" in result
            assert "current_db" not in result

    def test_get_full_schema_name_single_schema(self):
        """Single-part schema should prepend current_database (backward compatible)."""
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            result = dialect._get_full_schema_name(mock_conn, "my_schema")
            assert "current_db" in result
            assert "my_schema" in result

    def test_get_full_schema_name_no_schema(self):
        """No schema should use current_database and current_schema."""
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            result = dialect._get_full_schema_name(mock_conn, None)
            assert "current_db" in result
            assert "current_schema" in result

    def test_get_full_schema_name_invalid_raises(self):
        """Schema with >2 dot-separated parts should raise ValueError."""
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            with pytest.raises(ValueError, match="Invalid schema notation"):
                dialect._get_full_schema_name(mock_conn, "a.b.c")

    def test_query_all_columns_info_denormalizes_schema(self):
        """The WHERE clause must use a denormalized (uppercased) schema name.

        Snowflake stores TABLE_SCHEMA in UPPERCASE for case-insensitive identifiers
        in information_schema, and string comparisons are case-sensitive.
        """
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()

        # Call with a cross-database schema as _get_full_schema_name would produce
        dialect._query_all_columns_info(mock_conn, "test_db.my_schema", info_cache={})

        # Verify execute was called with UPPERCASED schema in the WHERE parameter
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        assert params["table_schema"] == "MY_SCHEMA"

        # Verify the FROM clause includes database-qualified information_schema
        sql_text = str(call_args[0][0])
        assert "test_db.information_schema.columns" in sql_text

    def test_query_all_columns_info_single_schema_denormalizes(self):
        """Single-part schema should also be denormalized for the WHERE clause."""
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()

        dialect._query_all_columns_info(mock_conn, "my_schema", info_cache={})

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        params = call_args[0][1]
        assert params["table_schema"] == "MY_SCHEMA"


class TestCrossDatabaseReflection:
    """Integration tests for cross-database reflection — requires Snowflake account."""

    @pytest.fixture()
    def setup_databases(self, engine_testaccount):
        """Create test databases, schemas, and tables for cross-database tests."""
        with engine_testaccount.connect() as conn:
            # All DDL uses fully-qualified names — no USE DATABASE needed
            conn.execute(text("CREATE DATABASE IF NOT EXISTS test_db_a"))
            conn.execute(text("CREATE DATABASE IF NOT EXISTS test_db_b"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS test_db_a.schema_a"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS test_db_b.schema_b"))
            conn.execute(
                text(
                    """
                    CREATE OR REPLACE TABLE test_db_a.schema_a.apples (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100),
                        color VARCHAR(50)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE OR REPLACE TABLE test_db_b.schema_b.bananas (
                        id INTEGER PRIMARY KEY,
                        variety VARCHAR(100),
                        ripeness INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text('CREATE SCHEMA IF NOT EXISTS test_db_b."schema.with.dots"')
            )
            conn.execute(
                text(
                    """
                    CREATE OR REPLACE TABLE test_db_b."schema.with.dots".oranges (
                        id INTEGER,
                        juice_content INTEGER
                    )
                    """
                )
            )
            conn.commit()

        yield

        with engine_testaccount.connect() as conn:
            conn.execute(text("DROP DATABASE IF EXISTS test_db_a"))
            conn.execute(text("DROP DATABASE IF EXISTS test_db_b"))
            conn.commit()

    def test_reflect_table_from_different_database(
        self, engine_testaccount, setup_databases
    ):
        """Test reflecting a table from database_b while connected to database_a."""
        metadata = MetaData()

        bananas = Table(
            "bananas",
            metadata,
            schema="test_db_b.schema_b",
            autoload_with=engine_testaccount,
        )

        assert "id" in bananas.columns
        assert "variety" in bananas.columns
        assert "ripeness" in bananas.columns
        assert bananas.columns["variety"].type.length == 100
        assert bananas.columns["id"].primary_key is True

    def test_cross_database_join(self, engine_testaccount, setup_databases):
        """Test joining tables from two different databases using CORE."""
        metadata = MetaData()

        apples = Table(
            "apples",
            metadata,
            schema="test_db_a.schema_a",
            autoload_with=engine_testaccount,
        )
        bananas = Table(
            "bananas",
            metadata,
            schema="test_db_b.schema_b",
            autoload_with=engine_testaccount,
        )

        stmt = select(apples, bananas).join(bananas, apples.c.id == bananas.c.id)

        compiled = stmt.compile(dialect=engine_testaccount.dialect)
        sql_str = str(compiled)

        assert "test_db_a.schema_a.apples" in sql_str
        assert "test_db_b.schema_b.bananas" in sql_str

    def test_metadata_reflect_cross_database(self, engine_testaccount, setup_databases):
        """Test metadata.reflect() with cross-database schema."""
        metadata = MetaData()

        metadata.reflect(
            bind=engine_testaccount,
            schema="test_db_b.schema_b",
            views=False,
        )

        table_key = "test_db_b.schema_b.bananas"
        assert table_key in metadata.tables
        bananas = metadata.tables[table_key]
        assert "variety" in bananas.columns

    def test_get_columns_cross_database(self, engine_testaccount, setup_databases):
        """Test inspector.get_columns with cross-database schema."""
        inspector = inspect(engine_testaccount)

        columns = inspector.get_columns("bananas", schema="test_db_b.schema_b")

        column_names = [col["name"] for col in columns]
        assert "id" in column_names
        assert "variety" in column_names
        assert "ripeness" in column_names

        variety_col = next(col for col in columns if col["name"] == "variety")
        assert variety_col["type"].length == 100

    def test_table_not_found_cross_database(self, engine_testaccount, setup_databases):
        """Test that NoSuchTableError is raised for non-existent cross-database table."""
        metadata = MetaData()

        with pytest.raises(NoSuchTableError):
            Table(
                "nonexistent_table",
                metadata,
                schema="test_db_b.schema_b",
                autoload_with=engine_testaccount,
            )

    def test_schema_with_quoted_dots(self, engine_testaccount, setup_databases):
        """Test reflecting a table from a schema whose name contains literal dots."""
        metadata = MetaData()
        oranges = Table(
            "oranges",
            metadata,
            schema='test_db_b."schema.with.dots"',
            autoload_with=engine_testaccount,
        )

        assert "id" in oranges.columns
        assert "juice_content" in oranges.columns

    def test_get_full_schema_name_with_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test _get_full_schema_name handles cross-database schemas correctly."""
        dialect = engine_testaccount.dialect

        with engine_testaccount.connect() as conn:
            # Test with database.schema notation
            full_name = dialect._get_full_schema_name(conn, "test_db_b.schema_b")
            assert "test_db_b" in full_name
            assert "schema_b" in full_name
            # Should NOT contain the current database prepended
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            current_db = result.scalar()
            assert not full_name.startswith(f'"{current_db.lower()}"."test_db_b"')

            # Test with single schema (backward compatibility)
            full_name = dialect._get_full_schema_name(conn, "some_schema")
            assert current_db.lower() in full_name.lower()
            assert "some_schema" in full_name

    def test_backward_compatibility_single_schema(self, engine_testaccount):
        """Verify single-part schema names still work (backward compatibility).

        This test does not need the cross-database fixture — it creates its own table.
        """
        metadata = MetaData()

        with engine_testaccount.connect() as conn:
            result = conn.execute(text("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()"))
            current_db, current_schema = result.one()

            table_name = "test_single_schema_table"
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE TABLE {table_name} (
                        id INTEGER,
                        data VARCHAR(100)
                    )
                    """
                )
            )
            conn.commit()

            try:
                table = Table(
                    table_name,
                    metadata,
                    schema=current_schema,
                    autoload_with=engine_testaccount,
                )

                assert "id" in table.columns
                assert "data" in table.columns

            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()
