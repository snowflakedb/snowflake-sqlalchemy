#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest
from sqlalchemy import MetaData, Table, inspect, select, text
from sqlalchemy.exc import NoSuchTableError

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


class TestCrossDatabaseReflection:
    """Tests for cross-database schema reflection using database.schema notation."""

    @pytest.fixture(autouse=True)
    def setup_databases(self, engine_testaccount):
        """Create test databases and schemas for cross-database tests."""
        with engine_testaccount.connect() as conn:
            # Get current database for cleanup
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            self.original_db = result.scalar()

            # Create test databases
            conn.execute(text("CREATE DATABASE IF NOT EXISTS test_db_a"))
            conn.execute(text("CREATE DATABASE IF NOT EXISTS test_db_b"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS test_db_a.schema_a"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS test_db_b.schema_b"))
            conn.commit()

            # Create test tables in different databases
            conn.execute(text("USE DATABASE test_db_a"))
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

            conn.execute(text("USE DATABASE test_db_b"))
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

            # Create a schema with dots in its name (quoted identifier)
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

            # Switch back to original database
            conn.execute(text(f"USE DATABASE {self.original_db}"))
            conn.commit()

        yield

        # Cleanup
        with engine_testaccount.connect() as conn:
            conn.execute(text("DROP DATABASE IF EXISTS test_db_a"))
            conn.execute(text("DROP DATABASE IF EXISTS test_db_b"))
            conn.commit()

    def test_reflect_table_from_different_database(self, engine_testaccount):
        """Test reflecting a table from database_b while connected to database_a."""
        metadata = MetaData()

        # Connect to test_db_a
        with engine_testaccount.connect() as conn:
            conn.execute(text("USE DATABASE test_db_a"))
            conn.commit()

            # Reflect table from test_db_b using database.schema notation
            bananas = Table(
                "bananas",
                metadata,
                schema="test_db_b.schema_b",
                autoload_with=engine_testaccount,
            )

            # Verify columns were reflected correctly
            assert "id" in bananas.columns
            assert "variety" in bananas.columns
            assert "ripeness" in bananas.columns
            assert bananas.columns["variety"].type.length == 100
            assert bananas.columns["id"].primary_key is True

    def test_cross_database_join(self, engine_testaccount):
        """Test joining tables from two different databases using CORE."""
        metadata = MetaData()

        # Reflect tables from both databases
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

        # Create a join query
        stmt = select(apples, bananas).join(bananas, apples.c.id == bananas.c.id)

        # Compile to SQL and verify database-qualified names
        compiled = stmt.compile(dialect=engine_testaccount.dialect)
        sql_str = str(compiled)

        assert "test_db_a.schema_a.apples" in sql_str
        assert "test_db_b.schema_b.bananas" in sql_str

    def test_backward_compatibility_single_schema(self, engine_testaccount):
        """Verify single-part schema names still work (backward compatibility)."""
        metadata = MetaData()

        with engine_testaccount.connect() as conn:
            # Get current database and schema
            result = conn.execute(text("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA()"))
            current_db, current_schema = result.one()

            # Create a table in the current schema
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
                # Reflect using single-part schema (backward compatible)
                table = Table(
                    table_name,
                    metadata,
                    schema=current_schema,
                    autoload_with=engine_testaccount,
                )

                # Verify reflection worked
                assert "id" in table.columns
                assert "data" in table.columns

            finally:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                conn.commit()

    def test_schema_parsing_variations(self, engine_testaccount):
        """Test various schema notation patterns."""
        dialect = SnowflakeDialect()

        # Test database.schema parsing
        parts = dialect.identifier_preparer._split_schema_by_dot("database.schema")
        assert len(parts) == 2
        assert str(parts[0]) == "database"
        assert str(parts[1]) == "schema"

        # Test quoted database and schema
        parts = dialect.identifier_preparer._split_schema_by_dot('"database"."schema"')
        assert len(parts) == 2
        assert str(parts[0]) == "database"
        assert str(parts[1]) == "schema"

        # Test schema with dots in quotes (should not split)
        parts = dialect.identifier_preparer._split_schema_by_dot('"schema.with.dots"')
        assert len(parts) == 1
        assert str(parts[0]) == "schema.with.dots"

        # Test reflecting table with quoted schema containing dots
        metadata = MetaData()
        oranges = Table(
            "oranges",
            metadata,
            schema='test_db_b."schema.with.dots"',
            autoload_with=engine_testaccount,
        )

        assert "id" in oranges.columns
        assert "juice_content" in oranges.columns

    def test_metadata_reflect_cross_database(self, engine_testaccount):
        """Test metadata.reflect() with cross-database schema."""
        metadata = MetaData()

        # Reflect all tables from test_db_b.schema_b
        metadata.reflect(
            bind=engine_testaccount,
            schema="test_db_b.schema_b",
            views=False,
        )

        # Verify the table was reflected with the correct schema
        table_key = "test_db_b.schema_b.bananas"
        assert table_key in metadata.tables
        bananas = metadata.tables[table_key]
        assert "variety" in bananas.columns

    def test_get_columns_cross_database(self, engine_testaccount):
        """Test inspector.get_columns with cross-database schema."""
        inspector = inspect(engine_testaccount)

        # Get columns from a table in a different database
        columns = inspector.get_columns("bananas", schema="test_db_b.schema_b")

        # Verify columns were retrieved
        column_names = [col["name"] for col in columns]
        assert "id" in column_names
        assert "variety" in column_names
        assert "ripeness" in column_names

        # Find the variety column and check its type
        variety_col = next(col for col in columns if col["name"] == "variety")
        assert variety_col["type"].length == 100

    def test_table_not_found_cross_database(self, engine_testaccount):
        """Test that NoSuchTableError is raised for non-existent cross-database table."""
        metadata = MetaData()

        with pytest.raises(NoSuchTableError):
            Table(
                "nonexistent_table",
                metadata,
                schema="test_db_b.schema_b",
                autoload_with=engine_testaccount,
            )

    def test_get_full_schema_name_with_cross_database(self, engine_testaccount):
        """Test _get_full_schema_name handles cross-database schemas correctly."""
        dialect = engine_testaccount.dialect

        with engine_testaccount.connect() as conn:
            # Test with database.schema notation
            full_name = dialect._get_full_schema_name(conn, "test_db_b.schema_b")
            # Should return the schema as-is, not prepend current database
            assert "test_db_b" in full_name
            assert "schema_b" in full_name
            # Should NOT contain the current database prepended
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            current_db = result.scalar()
            # The full name should not start with current_db.test_db_b
            assert not full_name.startswith(f'"{current_db.lower()}"."test_db_b"')

            # Test with single schema (backward compatibility)
            full_name = dialect._get_full_schema_name(conn, "some_schema")
            # Should prepend current database
            assert current_db.lower() in full_name.lower()
            assert "some_schema" in full_name
