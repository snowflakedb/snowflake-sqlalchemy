#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Tests for DDL option identifier quoting and string-literal escaping.
#
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, select
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import DynamicTable, IcebergTable, SnowflakeTable
from snowflake.sqlalchemy.sql.custom_schema.options import (
    ClusterByOption,
    IdentifierOption,
    KeywordOption,
    LiteralOption,
    SnowflakeKeyword,
    TableOptionKey,
    TimeUnit,
)


def test_identifier_option_quotes_special_chars(sql_compiler):
    """IdentifierOption must identifier-quote values that contain SQL metacharacters."""
    invalid_warehouse_name = "wh'; -- aaa"
    metadata = MetaData()
    table = DynamicTable(
        "t",
        metadata,
        Column("id", Integer),
        target_lag=(10, TimeUnit.SECONDS),
        warehouse=invalid_warehouse_name,
        as_query="SELECT 1",
    )
    sql = sql_compiler(CreateTable(table))
    assert f'"{invalid_warehouse_name}"' in sql


def test_identifier_option_doubles_embedded_quote(sql_compiler):
    """IdentifierOption must double any double-quote inside the identifier."""
    metadata = MetaData()
    table = DynamicTable(
        "t2",
        metadata,
        Column("id", Integer),
        target_lag=(10, TimeUnit.SECONDS),
        warehouse='my"warehouse',
        as_query="SELECT 1",
    )
    sql = sql_compiler(CreateTable(table))
    assert '"my""warehouse"' in sql


def test_identifier_option_bare_name_not_pre_quoted(sql_compiler):
    """IdentifierOption expects a bare name; the preparer quotes it when needed.

    A pre-quoted value (e.g. '"my_warehouse"') is not supported — the embedded
    quotes are treated as identifier characters and doubled.
    """
    metadata = MetaData()
    table = DynamicTable(
        "t_bare",
        metadata,
        Column("id", Integer),
        target_lag=(10, TimeUnit.SECONDS),
        warehouse="my_warehouse",
        as_query="SELECT 1",
    )
    sql = sql_compiler(CreateTable(table))
    assert "WAREHOUSE = my_warehouse" in sql


def test_literal_option_escapes_single_quote(sql_compiler):
    """LiteralOption must double single-quotes inside string literals."""
    metadata = MetaData()
    table = IcebergTable(
        "t3",
        metadata,
        Column("id", Integer),
        external_volume="vol",
        base_location="loc'; -- aaa",
    )
    sql = sql_compiler(CreateTable(table))
    assert "BASE_LOCATION = 'loc''; -- aaa'" in sql


def test_literal_option_escapes_backslash_before_quote(sql_compiler):
    r"""LiteralOption must double backslashes as well as single-quotes.

    With ESCAPE_STRING_LITERALS=TRUE, Snowflake reads \' as an escaped quote;
    doubling the backslash yields \\'' (literal backslash + escaped quote).
    """
    value_with_backslash_quote = "path\\' suffix"
    metadata = MetaData()
    table = IcebergTable(
        "t4",
        metadata,
        Column("id", Integer),
        external_volume="vol",
        base_location=value_with_backslash_quote,
    )
    sql = sql_compiler(CreateTable(table))
    assert "\\\\'' suffix" in sql


def test_cluster_by_option_quotes_special_chars(sql_compiler):
    """ClusterByOption must identifier-quote string column expressions."""
    invalid_column_name = "col); -- aaa"
    metadata = MetaData()
    table = SnowflakeTable(
        "t5",
        metadata,
        Column("id", Integer),
        cluster_by=ClusterByOption(invalid_column_name),
    )
    sql = sql_compiler(CreateTable(table))
    assert f'"{invalid_column_name}"' in sql


def test_as_query_selectable_uses_snowflake_dialect(sql_compiler):
    r"""AsQueryOption must compile Selectables with the active Snowflake dialect.

    SQLAlchemy's default dialect only doubles single-quotes, leaving backslashes
    raw, so a value containing \' could escape the literal. Compiling with
    the Snowflake dialect also doubles backslashes, yielding \\''.
    """
    value_with_backslash_quote = "val\\' x"
    src_meta = MetaData()
    source = Table("src", src_meta, Column("name", String))
    dynamic = DynamicTable(
        "t6",
        MetaData(),
        Column("name", String),
        target_lag=(10, TimeUnit.SECONDS),
        warehouse="wh",
        as_query=select(source).where(source.c.name == value_with_backslash_quote),
    )
    sql = sql_compiler(CreateTable(dynamic))
    assert "\\\\'' x" in sql


# Compiler-threading contract (SNOW-3649855): options handling user-controlled
# strings must use the compiler they receive. The marker preparer makes this
# checkable — QUOTED[…] appears only when compiler.preparer.quote() was called.
# KeywordOption (closed enum) and LiteralOption (pure escaping) are marker-free
# by design; AsQueryOption is covered separately above.


class _MarkerPreparer:
    """Preparer that wraps every quoted identifier in QUOTED[…] markers."""

    def quote(self, value):
        return f"QUOTED[{value}]"


class _MarkerCompiler:
    """Minimal compiler stub for verifying compiler-threading in _render."""

    preparer = _MarkerPreparer()

    class dialect:
        name = "snowflake"


_MARKER_COMPILER = _MarkerCompiler()


@pytest.mark.parametrize(
    "option, uses_compiler, expected_fragment",
    [
        pytest.param(
            IdentifierOption.create(TableOptionKey.WAREHOUSE, "my_wh"),
            True,
            "QUOTED[my_wh]",
            id="identifier_option_uses_preparer",
        ),
        pytest.param(
            ClusterByOption("col_a"),
            True,
            "QUOTED[col_a]",
            id="cluster_by_option_uses_preparer",
        ),
        pytest.param(
            LiteralOption.create(TableOptionKey.BASE_LOCATION, "some/path"),
            False,
            "BASE_LOCATION = 'some/path'",
            id="literal_option_no_compiler_dependency",
        ),
        pytest.param(
            KeywordOption.create(TableOptionKey.REFRESH_MODE, SnowflakeKeyword.AUTO),
            False,
            "REFRESH_MODE = AUTO",
            id="keyword_option_emits_bare_keyword",
        ),
    ],
)
def test_render_compiler_threading(option, uses_compiler, expected_fragment):
    """_render uses the compiler for user-controlled values; not for closed enums."""
    result = option._render(_MARKER_COMPILER)
    assert expected_fragment in result
    if uses_compiler:
        assert "QUOTED[" in result, (
            f"{type(option).__name__}._render did not use compiler.preparer.quote(); "
            f"got: {result!r}"
        )
