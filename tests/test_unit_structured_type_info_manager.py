#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""
Unit tests for _StructuredTypeInfoManager.get_table_columns identifier quoting.

These tests guard against the SQL injection vulnerability reported in
SNOW-3480955, where unquoted identifiers in the DESC TABLE fallback path
allowed arbitrary SQL to be injected via table_name / schema.

The tests assert the *correct* (post-fix) behaviour:
  - Every identifier component is double-quoted in the emitted SQL.
  - An injection payload embedded in a name is enclosed in double-quotes and
    cannot execute as a separate SQL statement.

All tests run without a live Snowflake connection — the connection is mocked
and we inspect the SQL string that would be sent to the server.
"""

import re
from unittest.mock import MagicMock

from snowflake.sqlalchemy.name_utils import _NameUtils
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from snowflake.sqlalchemy.structured_type_info_manager import _StructuredTypeInfoManager

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manager(default_schema="PUBLIC"):
    """Return a (_StructuredTypeInfoManager, captured_sql list) pair.

    The connection is mocked; every SQL string passed to _execute_desc is
    appended to *captured_sql* so tests can assert on what would be sent.
    """
    dialect = SnowflakeDialect()
    name_utils = _NameUtils(dialect.identifier_preparer)

    captured_sql: list[str] = []

    def _capture_execute(text_clause):
        captured_sql.append(text_clause.text)
        return iter([])  # empty result — _parse_desc_result returns []

    conn = MagicMock()
    conn.execute.side_effect = _capture_execute

    manager = _StructuredTypeInfoManager(conn, name_utils, default_schema)
    return manager, captured_sql


def _first_sql(captured: list[str]) -> str:
    assert captured, "No SQL was captured — _execute_desc was never called"
    return captured[0]


# ---------------------------------------------------------------------------
# Quoting correctness — normal identifiers
# ---------------------------------------------------------------------------


def test_plain_lowercase_identifiers_are_quoted():
    """Plain lowercase schema + table are uppercased then double-quoted."""
    manager, captured = _make_manager()
    manager.get_table_columns("mytable", schema="myschema")

    sql = _first_sql(captured)
    assert (
        '"MYSCHEMA"."MYTABLE"' in sql
    ), f"Expected double-quoted MYSCHEMA.MYTABLE in SQL, got: {sql!r}"


def test_plain_uppercase_identifiers_are_quoted():
    """Identifiers that arrive already uppercased are still double-quoted."""
    manager, captured = _make_manager()
    manager.get_table_columns("MYTABLE", schema="MYSCHEMA")

    sql = _first_sql(captured)
    assert (
        '"MYSCHEMA"."MYTABLE"' in sql
    ), f"Expected double-quoted MYSCHEMA.MYTABLE in SQL, got: {sql!r}"


def test_mixed_case_identifier_is_quoted():
    """Mixed-case names that require quoting are enclosed in double-quotes."""
    manager, captured = _make_manager()
    manager.get_table_columns("MyTable", schema="MySchema")

    sql = _first_sql(captured)
    # Mixed-case names are returned unchanged by denormalize_name; ip.quote
    # wraps them in double-quotes because lc != value.
    assert '"MyTable"' in sql, f'Expected "MyTable" quoted in SQL, got: {sql!r}'
    assert '"MySchema"' in sql, f'Expected "MySchema" quoted in SQL, got: {sql!r}'


def test_default_schema_used_when_schema_is_none():
    """When schema=None the default_schema provided at construction is used."""
    manager, captured = _make_manager(default_schema="myschema")
    manager.get_table_columns("mytable", schema=None)

    sql = _first_sql(captured)
    assert (
        '"MYSCHEMA"' in sql
    ), f"Expected default schema MYSCHEMA to appear quoted, got: {sql!r}"
    assert '"MYTABLE"' in sql


def test_dotted_table_name_uses_only_last_component():
    """A table_name of 'schema.table' splits on '.'; only the last part is used."""
    manager, captured = _make_manager()
    manager.get_table_columns("myschema.mytable", schema="myschema")

    sql = _first_sql(captured)
    # Exactly one schema qualifier — 'myschema' should not appear twice
    assert (
        sql.count('"MYSCHEMA"') == 1
    ), f"Schema appeared more than once in SQL, got: {sql!r}"
    assert '"MYTABLE"' in sql


# ---------------------------------------------------------------------------
# SQL injection — table_name payload
# ---------------------------------------------------------------------------


def test_semicolon_in_table_name_is_enclosed_in_double_quotes():
    """Semicolons in table_name must not escape double-quote enclosure (SNOW-3480955)."""
    payload = "foo; SELECT 1--"
    manager, captured = _make_manager()
    manager.get_table_columns(payload, schema="myschema")

    sql = _first_sql(captured)
    # The payload must appear inside double-quotes in the SQL string
    assert (
        f'"{payload}"' in sql
    ), f"Payload was not enclosed in double-quotes; got: {sql!r}"
    # No bare semicolon must appear outside a quoted identifier
    # Strip everything inside double-quoted segments, then check for semicolons
    stripped = re.sub(r'"[^"]*"', "", sql)
    assert (
        ";" not in stripped
    ), f"Semicolon escaped double-quote enclosure; got: {sql!r}"


def test_drop_statement_injection_in_table_name_is_quoted():
    """Classic DROP TABLE injection payload in table_name is neutralised by quoting."""
    payload = "x'; DROP TABLE users--"
    manager, captured = _make_manager()
    manager.get_table_columns(payload, schema="myschema")

    sql = _first_sql(captured)
    assert f'"{payload}"' in sql, f"Payload not enclosed in double-quotes; got: {sql!r}"
    stripped = re.sub(r'"[^"]*"', "", sql)
    assert (
        "DROP" not in stripped
    ), f"DROP keyword escaped double-quote enclosure; got: {sql!r}"


def test_newline_injection_in_table_name_is_quoted():
    """Newline characters in table_name are enclosed in double-quotes."""
    payload = "foo\nSELECT 1"
    manager, captured = _make_manager()
    manager.get_table_columns(payload, schema="myschema")

    sql = _first_sql(captured)
    assert f'"{payload}"' in sql, f"Payload not enclosed in double-quotes; got: {sql!r}"


# ---------------------------------------------------------------------------
# SQL injection — schema payload
# ---------------------------------------------------------------------------


def test_semicolon_in_schema_is_enclosed_in_double_quotes():
    """Injection payload in schema is also double-quoted (SNOW-3480955)."""
    payload = "myschema; SELECT 1--"
    manager, captured = _make_manager()
    manager.get_table_columns("mytable", schema=payload)

    sql = _first_sql(captured)
    assert (
        f'"{payload}"' in sql
    ), f"Schema payload not enclosed in double-quotes; got: {sql!r}"
    stripped = re.sub(r'"[^"]*"', "", sql)
    assert (
        ";" not in stripped
    ), f"Semicolon escaped double-quote enclosure in schema; got: {sql!r}"


# ---------------------------------------------------------------------------
# DESC TABLE statement shape
# ---------------------------------------------------------------------------


def test_desc_table_statement_structure():
    """The emitted SQL is a DESC TABLE … TYPE = COLUMNS statement."""
    manager, captured = _make_manager()
    manager.get_table_columns("mytable", schema="myschema")

    sql = _first_sql(captured)
    assert sql.startswith("DESC"), f"Expected DESC statement, got: {sql!r}"
    assert "TYPE = COLUMNS" in sql, f"Missing TYPE = COLUMNS clause, got: {sql!r}"
