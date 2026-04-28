#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import uuid
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import MetaData, Table, inspect, text
from sqlalchemy.exc import NoSuchTableError
from sqlalchemy.sql.elements import TextClause

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
        "schema, expected",
        [
            ("test_db.my_schema", '"TEST_DB"."MY_SCHEMA"'),
            ('"test_db"."MiXeD"', '"TEST_DB"."MiXeD"'),
            ("my_schema", '"CURRENT_DB"."MY_SCHEMA"'),
            (None, '"CURRENT_DB"."CURRENT_SCHEMA"'),
        ],
        ids=["cross_database", "quoted_mixed", "single_schema", "no_schema"],
    )
    def test_get_full_schema_name(self, schema, expected):
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            result = dialect._get_full_schema_name(mock_conn, schema)
        assert result == expected

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
            (
                '"test_db"."schema.with.dots"',
                "schema.with.dots",
                "test_db.information_schema.columns",
            ),
            ("test_db.my_schema", "MY_SCHEMA", "test_db.information_schema.columns"),
        ],
        ids=["quoted_dot_schema", "cross_database"],
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

    def test_query_all_columns_info_requires_fully_qualified_schema(self):
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()

        with pytest.raises(ValueError, match="Expected fully-qualified schema name"):
            dialect._query_all_columns_info(mock_conn, "my_schema", info_cache={})

    @pytest.mark.parametrize(
        "method_name, expected_fragment",
        [
            (
                "get_multi_pk_constraint",
                'PRIMARY KEYS IN SCHEMA "TEST_DB"."TEST_SCHEMA"',
            ),
            (
                "get_multi_unique_constraints",
                'UNIQUE KEYS IN SCHEMA "TEST_DB"."TEST_SCHEMA"',
            ),
            (
                "get_multi_foreign_keys",
                'IMPORTED KEYS IN SCHEMA "TEST_DB"."TEST_SCHEMA"',
            ),
            (
                "get_sequence_names",
                'SHOW SEQUENCES IN SCHEMA "TEST_DB"."TEST_SCHEMA"',
            ),
            (
                "get_table_names",
                'TABLES IN SCHEMA "TEST_DB"."TEST_SCHEMA"',
            ),
            (
                "get_table_comment",
                'TABLES LIKE \'test_table\' IN SCHEMA "TEST_DB"."TEST_SCHEMA"',
            ),
        ],
    )
    def test_reflection_uses_full_schema_name_without_re_denormalizing(
        self, method_name, expected_fragment
    ):
        dialect = SnowflakeDialect()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.cursor.description = []
        mock_result.cursor.fetchall.return_value = []
        mock_result.fetchone.return_value = None
        mock_conn.execute.return_value = mock_result

        with patch.object(
            dialect,
            "_current_database_schema",
            return_value=("current_db", "current_schema"),
        ):
            method = getattr(dialect, method_name)
            if method_name.startswith("get_multi_"):
                method(mock_conn, schema="test_db.test_schema")
            elif method_name == "get_table_comment":
                method(mock_conn, "test_table", schema="test_db.test_schema")
            else:
                method(mock_conn, schema="test_db.test_schema")

        statements = [call.args[0] for call in mock_conn.execute.call_args_list]
        assert all(isinstance(statement, TextClause) for statement in statements)
        rendered = [str(statement) for statement in statements]
        assert any(expected_fragment in statement for statement in rendered)
        assert all('"testdb_test_schema"' not in statement for statement in rendered)


class TestCrossDatabaseReflection:
    """Integration tests for cross-database reflection — requires Snowflake account."""

    @pytest.fixture()
    def setup_databases(self, engine_testaccount):
        """Create schemas and tables for fully-qualified schema reflection tests.

        CI roles may not have `CREATE DATABASE` privileges, so this fixture reuses
        the current database and creates isolated schemas inside it. The tests still
        exercise the changed behavior because they pass a fully-qualified
        `database.schema` name to reflection APIs.
        """
        suffix = uuid.uuid4().hex[:8]
        schema_a = f"schema_a_{suffix}"
        schema_b = f"schema_b_{suffix}"
        dotted_schema = f'"schema.with.dots.{suffix}"'

        with engine_testaccount.connect() as conn:
            current_db = conn.execute(text("SELECT CURRENT_DATABASE()")).scalar()
            # All DDL uses fully-qualified names — no USE DATABASE needed
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {current_db}.{schema_a}"))
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {current_db}.{schema_b}"))
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE TABLE {current_db}.{schema_a}.apples (
                        id INTEGER PRIMARY KEY,
                        name VARCHAR(100),
                        color VARCHAR(50)
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE TABLE {current_db}.{schema_b}.bananas (
                        id INTEGER PRIMARY KEY,
                        variety VARCHAR(100) UNIQUE,
                        ripeness INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE TABLE {current_db}.{schema_b}.banana_ratings (
                        rating_id INTEGER PRIMARY KEY,
                        banana_id INTEGER,
                        score INTEGER,
                        FOREIGN KEY (banana_id) REFERENCES {current_db}.{schema_b}.bananas(id)
                    )
                    """
                )
            )
            conn.execute(
                text(f"CREATE SCHEMA IF NOT EXISTS {current_db}.{dotted_schema}")
            )
            conn.execute(
                text(
                    f"""
                    CREATE OR REPLACE TABLE {current_db}.{dotted_schema}.oranges (
                        id INTEGER,
                        juice_content INTEGER
                    )
                    """
                )
            )
            conn.commit()

        yield {
            "db_a": current_db,
            "db_b": current_db,
            "schema_a": schema_a,
            "schema_b": schema_b,
            "dotted_schema": dotted_schema.strip('"'),
        }

        with engine_testaccount.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {current_db}.{schema_a}"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {current_db}.{schema_b}"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {current_db}.{dotted_schema}"))
            conn.commit()

    def test_reflect_cross_database_table(self, engine_testaccount, setup_databases):
        """Test reflecting a table using database.schema notation."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        metadata = MetaData()

        table = Table(
            "bananas",
            metadata,
            schema=f"{db_b}.{schema_b}",
            autoload_with=engine_testaccount,
        )
        for col in ["id", "variety", "ripeness"]:
            assert col in table.columns

    def test_reflect_cross_database_table_quoted_dot_schema(
        self, engine_testaccount, setup_databases
    ):
        """Test reflecting a table with a dot-containing schema name."""
        db_b = setup_databases["db_b"]
        dotted_schema = setup_databases["dotted_schema"]
        metadata = MetaData()

        table = Table(
            "oranges",
            metadata,
            schema=f'{db_b}."{dotted_schema}"',
            autoload_with=engine_testaccount,
        )
        for col in ["id", "juice_content"]:
            assert col in table.columns

    def test_reflect_cross_database_column_details(
        self, engine_testaccount, setup_databases
    ):
        """Verify reflected column types and primary key from a cross-database table."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        metadata = MetaData()
        bananas = Table(
            "bananas",
            metadata,
            schema=f"{db_b}.{schema_b}",
            autoload_with=engine_testaccount,
        )
        assert bananas.columns["variety"].type.length == 100
        assert bananas.columns["id"].primary_key is True

    def test_metadata_reflect_cross_database(self, engine_testaccount, setup_databases):
        """Test metadata.reflect() with cross-database schema."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        metadata = MetaData()

        metadata.reflect(
            bind=engine_testaccount,
            schema=f"{db_b}.{schema_b}",
            views=False,
        )

        table_key = f"{db_b}.{schema_b}.bananas"
        assert table_key in metadata.tables
        assert "variety" in metadata.tables[table_key].columns

    def test_get_columns_cross_database(self, engine_testaccount, setup_databases):
        """Test inspector.get_columns with cross-database schema."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        inspector = inspect(engine_testaccount)

        columns = inspector.get_columns("bananas", schema=f"{db_b}.{schema_b}")

        column_names = [col["name"] for col in columns]
        assert "id" in column_names
        assert "variety" in column_names
        assert "ripeness" in column_names

        variety_col = next(col for col in columns if col["name"] == "variety")
        assert variety_col["type"].length == 100

    def test_table_not_found_cross_database(self, engine_testaccount, setup_databases):
        """Test that NoSuchTableError is raised for non-existent cross-database table."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        metadata = MetaData()

        with pytest.raises(NoSuchTableError):
            Table(
                "nonexistent_table",
                metadata,
                schema=f"{db_b}.{schema_b}",
                autoload_with=engine_testaccount,
            )

    def test_get_full_schema_name_with_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test _get_full_schema_name handles cross-database schemas correctly."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        dotted_schema = setup_databases["dotted_schema"]
        dialect = engine_testaccount.dialect

        with engine_testaccount.connect() as conn:
            full_name = dialect._get_full_schema_name(conn, f"{db_b}.{schema_b}")
            assert db_b in full_name
            assert schema_b.upper() in full_name
            result = conn.execute(text("SELECT CURRENT_DATABASE()"))
            current_db = result.scalar()
            assert full_name == f'"{db_b}"."{schema_b.upper()}"'

            full_name = dialect._get_full_schema_name(conn, "some_schema")
            assert current_db.lower() in full_name.lower()
            assert "some_schema" in full_name.lower()

            quoted_full_name = dialect._get_full_schema_name(
                conn, f'"{db_b}"."{dotted_schema}"'
            )
            assert quoted_full_name == f'"{db_b}"."{dotted_schema}"'

    def test_get_pk_constraint_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test inspector.get_pk_constraint with cross-database schema."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        inspector = inspect(engine_testaccount)

        pk = inspector.get_pk_constraint("bananas", schema=f"{db_b}.{schema_b}")

        assert "id" in pk["constrained_columns"]

    def test_get_unique_constraints_cross_database(
        self, engine_testaccount, setup_databases
    ):
        """Test inspector.get_unique_constraints with cross-database schema."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        inspector = inspect(engine_testaccount)

        ucs = inspector.get_unique_constraints("bananas", schema=f"{db_b}.{schema_b}")

        uc_columns = [col for uc in ucs for col in uc["column_names"]]
        assert "variety" in uc_columns

    def test_get_foreign_keys_cross_database(self, engine_testaccount, setup_databases):
        """Test inspector.get_foreign_keys with cross-database schema."""
        db_b = setup_databases["db_b"]
        schema_b = setup_databases["schema_b"]
        inspector = inspect(engine_testaccount)

        fks = inspector.get_foreign_keys("banana_ratings", schema=f"{db_b}.{schema_b}")

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
