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

    @pytest.mark.parametrize(
        "input_schema, expected_parts",
        [
            ("database.schema", ["database", "schema"]),
            ('"database"."schema"', ["database", "schema"]),
            ('"schema.with.dots"', ["schema.with.dots"]),
            ('test_db."schema.with.dots"', ["test_db", "schema.with.dots"]),
        ],
        ids=["unquoted", "quoted", "quoted_dots", "mixed"],
    )
    def test_split_schema_by_dot(self, input_schema, expected_parts):
        dialect = SnowflakeDialect()
        parts = dialect.identifier_preparer._split_schema_by_dot(input_schema)
        assert len(parts) == len(expected_parts)
        assert [str(p) for p in parts] == expected_parts

    @pytest.mark.parametrize(
        "schema, expected_present, expected_absent",
        [
            ("test_db.my_schema", ["test_db", "my_schema"], ["current_db"]),
            ("my_schema", ["current_db", "my_schema"], []),
            (None, ["current_db", "current_schema"], []),
        ],
        ids=["cross_database", "single_schema", "no_schema"],
    )
    def test_get_full_schema_name(self, schema, expected_present, expected_absent):
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            result = dialect._get_full_schema_name(mock_conn, schema)
            for token in expected_present:
                assert token in result, f"Expected '{token}' in '{result}'"
            for token in expected_absent:
                assert token not in result, f"Expected '{token}' NOT in '{result}'"

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

    @pytest.mark.parametrize(
        "schema_name, expected_table_schema, expected_from_fragment",
        [
            ("test_db.my_schema", "MY_SCHEMA", "test_db.information_schema.columns"),
            ("my_schema", "MY_SCHEMA", "information_schema.columns"),
        ],
        ids=["cross_database", "single_schema"],
    )
    def test_query_all_columns_info_denormalizes_schema(
        self, schema_name, expected_table_schema, expected_from_fragment
    ):
        """The WHERE clause must use a denormalized (uppercased) schema name.

        Snowflake stores TABLE_SCHEMA in UPPERCASE for case-insensitive identifiers
        in information_schema, and string comparisons are case-sensitive.
        """
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()

        dialect._query_all_columns_info(mock_conn, schema_name, info_cache={})

        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert call_args[0][1]["table_schema"] == expected_table_schema
        assert expected_from_fragment in str(call_args[0][0])


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
                        variety VARCHAR(100) UNIQUE,
                        ripeness INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE OR REPLACE TABLE test_db_b.schema_b.banana_ratings (
                        rating_id INTEGER PRIMARY KEY,
                        banana_id INTEGER,
                        score INTEGER,
                        FOREIGN KEY (banana_id) REFERENCES test_db_b.schema_b.bananas(id)
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

    @pytest.mark.parametrize(
        "table_name, schema, expected_columns",
        [
            ("bananas", "test_db_b.schema_b", ["id", "variety", "ripeness"]),
            ("oranges", 'test_db_b."schema.with.dots"', ["id", "juice_content"]),
        ],
        ids=["cross_database", "quoted_dot_schema"],
    )
    def test_reflect_cross_database_table(
        self, engine_testaccount, setup_databases, table_name, schema, expected_columns
    ):
        """Test reflecting a table using database.schema notation."""
        metadata = MetaData()
        table = Table(
            table_name,
            metadata,
            schema=schema,
            autoload_with=engine_testaccount,
        )
        for col in expected_columns:
            assert col in table.columns

    def test_reflect_cross_database_column_details(
        self, engine_testaccount, setup_databases
    ):
        """Verify reflected column types and primary key from a cross-database table."""
        metadata = MetaData()
        bananas = Table(
            "bananas",
            metadata,
            schema="test_db_b.schema_b",
            autoload_with=engine_testaccount,
        )
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
        assert "variety" in metadata.tables[table_key].columns

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

    def test_get_full_schema_name_with_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test _get_full_schema_name handles cross-database schemas correctly."""
        dialect = engine_testaccount.dialect

        with engine_testaccount.connect() as conn:
            full_name = dialect._get_full_schema_name(conn, "test_db_b.schema_b")
            assert "test_db_b" in full_name
            assert "schema_b" in full_name
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            current_db = result.scalar()
            assert not full_name.startswith(f'"{current_db.lower()}"."test_db_b"')

            full_name = dialect._get_full_schema_name(conn, "some_schema")
            assert current_db.lower() in full_name.lower()
            assert "some_schema" in full_name

    def test_get_pk_constraint_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test inspector.get_pk_constraint with cross-database schema."""
        inspector = inspect(engine_testaccount)

        pk = inspector.get_pk_constraint("bananas", schema="test_db_b.schema_b")

        assert "id" in pk["constrained_columns"]

    def test_get_unique_constraints_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test inspector.get_unique_constraints with cross-database schema."""
        inspector = inspect(engine_testaccount)

        ucs = inspector.get_unique_constraints("bananas", schema="test_db_b.schema_b")

        uc_columns = [col for uc in ucs for col in uc["column_names"]]
        assert "variety" in uc_columns

    def test_get_foreign_keys_cross_database(self, engine_testaccount, setup_databases):
        """Test inspector.get_foreign_keys with cross-database schema."""
        inspector = inspect(engine_testaccount)

        fks = inspector.get_foreign_keys("banana_ratings", schema="test_db_b.schema_b")

        assert len(fks) >= 1
        fk = fks[0]
        assert fk["constrained_columns"] == ["banana_id"]
        assert fk["referred_table"] == "bananas"
        assert "id" in fk["referred_columns"]

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
