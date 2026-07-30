#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""Unit tests for paginated table reflection (SNOW-796954 / GH #406).

Snowflake ``SHOW`` commands cap output at 10,000 rows, so ``_get_schema_tables_info``
pages through ``SHOW TABLES`` with ``LIMIT ... FROM ...``. The 10k cap is server-side,
so these tests never create real tables: they mock ``connection.execute`` with tiny
synthetic result sets and shrink ``_SHOW_TABLES_PAGE_SIZE`` so multi-page behavior is
exercised with a handful of rows.
"""

from unittest.mock import MagicMock

import pytest

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

# Column layout of ``SHOW TABLES`` output that _map_name_to_idx / get_prefixes_from_data
# rely on: index 1 is ``name`` and the ``is_*`` flags drive prefix detection.
SHOW_DESCRIPTION = [
    ("created_on",),
    ("name",),
    ("database_name",),
    ("schema_name",),
    ("kind",),
    ("comment",),
    ("cluster_by",),
    ("rows",),
    ("bytes",),
    ("owner",),
    ("retention_time",),
    ("is_external",),
    ("is_event",),
    ("is_hybrid",),
    ("is_iceberg",),
    ("is_dynamic",),
]
_IDX = {col[0]: i for i, col in enumerate(SHOW_DESCRIPTION)}


def _row(name, *, hybrid=False, iceberg=False, dynamic=False, kind="TABLE"):
    row = [None] * len(SHOW_DESCRIPTION)
    row[_IDX["name"]] = name
    row[_IDX["kind"]] = kind
    row[_IDX["is_external"]] = "N"
    row[_IDX["is_event"]] = "N"
    row[_IDX["is_hybrid"]] = "Y" if hybrid else "N"
    row[_IDX["is_iceberg"]] = "Y" if iceberg else "N"
    row[_IDX["is_dynamic"]] = "Y" if dynamic else "N"
    return row


def _result_with(rows, description):
    """Mock CursorResult with an arbitrary column description."""
    result = MagicMock()
    result.cursor.description = description
    result.cursor.fetchall.return_value = rows
    return result


def _dialect_with_results(results, page_size=2):
    """Dialect whose connection.execute yields each prebuilt result in turn."""
    dialect = SnowflakeDialect()
    dialect._SHOW_TABLES_PAGE_SIZE = page_size
    dialect._get_full_schema_name = MagicMock(return_value='"MYDB"."MYSCHEMA"')
    conn = MagicMock()
    conn.execute.side_effect = list(results)
    return dialect, conn


def _result_for(rows):
    result = MagicMock()
    result.cursor.description = SHOW_DESCRIPTION
    result.cursor.fetchall.return_value = rows
    return result


def _dialect_with_pages(pages, page_size=2):
    """A dialect whose connection returns each page in turn from execute()."""
    dialect = SnowflakeDialect()
    dialect._SHOW_TABLES_PAGE_SIZE = page_size
    dialect._get_full_schema_name = MagicMock(return_value='"MYDB"."MYSCHEMA"')

    conn = MagicMock()
    conn.execute.side_effect = [_result_for(p) for p in pages]
    return dialect, conn


def test_single_page_partial_stops_immediately():
    """A page smaller than page_size ends pagination after one call."""
    dialect, conn = _dialect_with_pages([[_row("ALPHA")]], page_size=2)
    tables = dialect._get_schema_tables_info(conn, schema="myschema")
    assert set(tables) == {"alpha"}
    assert conn.execute.call_count == 1
    assert "LIMIT 2" in str(conn.execute.call_args_list[0][0][0])
    # First page must not carry a FROM cursor.
    assert "FROM" not in str(conn.execute.call_args_list[0][0][0])


def test_multi_page_collects_all_rows():
    """Full pages are followed until a short page is returned (5 rows / 3 pages)."""
    pages = [
        [_row("ALPHA"), _row("BRAVO")],  # full
        [_row("CHARLIE"), _row("DELTA")],  # full
        [_row("ECHO")],  # partial -> stop
    ]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    tables = dialect._get_schema_tables_info(conn, schema="myschema")
    assert set(tables) == {"alpha", "bravo", "charlie", "delta", "echo"}
    assert conn.execute.call_count == 3


def test_exact_page_size_triggers_extra_empty_fetch():
    """When the last data page is exactly full, one more (empty) fetch ends the loop."""
    pages = [
        [_row("TABLE_A"), _row("TABLE_B")],  # full
        [_row("TABLE_C"), _row("TABLE_D")],  # full
        [],  # empty -> stop
    ]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    tables = dialect._get_schema_tables_info(conn, schema="myschema")
    assert len(tables) == 4
    assert conn.execute.call_count == 3


def test_from_cursor_uses_last_raw_name():
    """Page 2+ pages with FROM '<last raw name from previous page>'."""
    pages = [[_row("Alpha"), _row("Bravo")], [_row("Charlie")]]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    dialect._get_schema_tables_info(conn, schema="myschema")

    sql_page1 = str(conn.execute.call_args_list[0][0][0])
    sql_page2 = str(conn.execute.call_args_list[1][0][0])
    assert "FROM" not in sql_page1
    # Raw (non-normalized) name is used for the case-sensitive FROM literal.
    assert "FROM 'Bravo'" in sql_page2


def test_empty_schema_returns_empty_dict():
    dialect, conn = _dialect_with_pages([[]], page_size=2)
    tables = dialect._get_schema_tables_info(conn, schema="myschema")
    assert tables == {}
    assert conn.execute.call_count == 1


def test_prefixes_preserved_across_pages():
    """HYBRID/ICEBERG/DYNAMIC detection survives pagination."""
    pages = [
        [_row("H", hybrid=True), _row("I", iceberg=True)],  # full
        [_row("D", dynamic=True)],  # partial -> stop
    ]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    tables = dialect._get_schema_tables_info(conn, schema="myschema")
    assert tables["h"]["prefixes"] == ["HYBRID"]
    assert tables["i"]["prefixes"] == ["ICEBERG"]
    assert tables["d"]["prefixes"] == ["DYNAMIC"]


def test_from_literal_is_escaped():
    """A single quote in a table name is escaped inside the FROM literal."""
    pages = [[_row("O'HARA"), _row("PLAIN")], [_row("ZED")]]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    dialect._get_schema_tables_info(conn, schema="myschema")
    sql_page2 = str(conn.execute.call_args_list[1][0][0])
    # escape_string_literal_interior doubles the quote so the literal stays valid.
    assert "FROM 'PLAIN'" in sql_page2  # last row of page 1 drives the cursor

    # And a quote in the cursor name is doubled, not left raw.
    pages2 = [[_row("A"), _row("O'HARA")], [_row("B")]]
    dialect2, conn2 = _dialect_with_pages(pages2, page_size=2)
    dialect2._get_schema_tables_info(conn2, schema="myschema")
    assert "FROM 'O''HARA'" in str(conn2.execute.call_args_list[1][0][0])


def test_get_table_names_returns_keys():
    pages = [[_row("ALPHA"), _row("BRAVO")], [_row("CHARLIE")]]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    names = dialect.get_table_names(conn, schema="myschema")
    assert sorted(names) == ["alpha", "bravo", "charlie"]


def test_get_table_names_with_prefix_filters():
    pages = [[_row("PLAIN"), _row("HYB", hybrid=True)]]
    dialect, conn = _dialect_with_pages(pages, page_size=5)
    hybrids = dialect.get_table_names_with_prefix(
        conn, schema="myschema", prefix="HYBRID"
    )
    assert hybrids == ["hyb"]


@pytest.mark.parametrize("n", [1, 2, 3])
def test_default_page_size_is_10000(n):
    """The production default remains 10,000 (single fetch for small schemas)."""
    dialect = SnowflakeDialect()
    assert dialect._SHOW_TABLES_PAGE_SIZE == 10000
    dialect._get_full_schema_name = MagicMock(return_value='"D"."S"')
    conn = MagicMock()
    conn.execute.side_effect = [_result_for([_row(f"T{i}") for i in range(n)])]
    tables = dialect._get_schema_tables_info(conn, schema="s")
    assert len(tables) == n
    assert conn.execute.call_count == 1
    assert "LIMIT 10000" in str(conn.execute.call_args_list[0][0][0])


def test_page_size_zero_is_clamped_to_one():
    """A misconfigured page size of 0 must not crash (clamped to >=1)."""
    dialect, conn = _dialect_with_pages([[_row("A")], []], page_size=0)
    tables = dialect._get_schema_tables_info(conn, schema="myschema")
    assert set(tables) == {"a"}
    assert "LIMIT 1" in str(conn.execute.call_args_list[0][0][0])


def test_page_size_over_max_is_clamped_to_10000():
    """Snowflake rejects LIMIT > 10000, so an oversized override is clamped."""
    dialect, conn = _dialect_with_pages([[_row("A")]], page_size=25000)
    dialect._get_schema_tables_info(conn, schema="myschema")
    assert "LIMIT 10000" in str(conn.execute.call_args_list[0][0][0])


def test_non_advancing_cursor_does_not_loop_forever():
    """If SHOW keeps returning the same full page, the guard stops the loop."""
    dialect = SnowflakeDialect()
    dialect._SHOW_TABLES_PAGE_SIZE = 2
    dialect._get_full_schema_name = MagicMock(return_value='"D"."S"')
    conn = MagicMock()
    # Infinite supply of an identical full page (last name never advances).
    conn.execute.side_effect = lambda *a, **k: _result_for([_row("A"), _row("B")])
    tables = dialect._get_schema_tables_info(conn, schema="s")
    assert set(tables) == {"a", "b"}
    # Page 1 sets cursor 'B'; page 2 yields 'B' again -> guard breaks. No hang.
    assert conn.execute.call_count == 2


def test_from_cursor_escapes_backslash():
    """A backslash in the cursor name is doubled in the FROM literal."""
    pages = [[_row("A"), _row("back\\slash")], [_row("Z")]]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    dialect._get_schema_tables_info(conn, schema="myschema")
    sql_page2 = str(conn.execute.call_args_list[1][0][0])
    assert "FROM 'back\\\\slash'" in sql_page2


@pytest.mark.parametrize(
    "evil_name",
    [
        "tab'; DROP TABLE x; --",  # quote-breakout + statement injection
        "trailing_backslash\\",  # trailing \ must not escape the closing quote
        "quote'inside",  # embedded single quote
        "backslash_quote\\'",  # \ immediately before a quote
        "both\\'; SELECT 1; --",  # combined backslash + quote payload
    ],
)
def test_from_cursor_is_injection_safe(evil_name):
    """The FROM cursor fully escapes both ' and \\ so no payload can break out.

    Locks in the secure ``escape_string_literal_interior`` behavior: switching to
    single-quote-only escaping (which would leave a trailing backslash able to
    escape the closing quote) makes this test fail.
    """
    from snowflake.sqlalchemy.util import escape_string_literal_interior

    # Put the hostile name last on page 1 so it becomes the FROM cursor.
    pages = [[_row("aaa"), _row(evil_name)], [_row("zzz")]]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    dialect._get_schema_tables_info(conn, schema="myschema")

    sql_page2 = str(conn.execute.call_args_list[1][0][0])
    expected = f"FROM '{escape_string_literal_interior(evil_name)}'"
    assert expected in sql_page2

    # The literal body must have every quote and backslash doubled (even counts),
    # so the single-quoted string cannot be terminated early.
    body = sql_page2.split("FROM '", 1)[1].rsplit("'", 1)[0]
    assert body.count("'") % 2 == 0
    assert (len(body) - len(body.replace("\\", ""))) % 2 == 0


def test_pagination_traverses_real_catalog_semantics():
    """End-to-end pagination against a fake server that honors LIMIT/FROM.

    This models Snowflake's documented contract (FROM returns names strictly
    greater than the cursor, ordered lexicographically) rather than blindly
    replaying canned pages, proving the cursor logic actually traverses.
    """
    import re

    catalog = sorted(f"T{i:03d}" for i in range(23))  # 23 tables, page size 5

    dialect = SnowflakeDialect()
    dialect._SHOW_TABLES_PAGE_SIZE = 5
    dialect._get_full_schema_name = MagicMock(return_value='"D"."S"')

    def fake_execute(stmt, *a, **k):
        sql = str(stmt)
        m = re.search(r"LIMIT (\d+)", sql)
        limit = int(m.group(1))
        cursor = None
        fm = re.search(r"FROM '([^']*)'", sql)
        if fm:
            cursor = fm.group(1)
        after = [n for n in catalog if cursor is None or n > cursor]
        return _result_for([_row(n) for n in after[:limit]])

    conn = MagicMock()
    conn.execute.side_effect = fake_execute
    tables = dialect._get_schema_tables_info(conn, schema="s")

    assert set(tables) == {n.lower() for n in catalog}
    # 23 rows / 5 per page = 5 pages (last partial), so 5 execute calls.
    assert conn.execute.call_count == 5


# --- Other object-listing SHOW commands now share the pagination helper -------

# SHOW VIEWS / SHOW SCHEMAS expose `name` at column index 1.
_NAME_AT_1 = [("created_on",), ("name",)]
# SHOW SEQUENCES: get_sequence_names reads column 0, which is also `name`.
_NAME_AT_0 = [("name",), ("created_on",)]


def test_get_view_names_paginates():
    results = [
        _result_with([["c", "AV"], ["c", "BV"]], _NAME_AT_1),  # full
        _result_with([["c", "CV"]], _NAME_AT_1),  # partial -> stop
    ]
    dialect, conn = _dialect_with_results(results, page_size=2)
    names = dialect.get_view_names(conn, schema="myschema")
    assert names == ["av", "bv", "cv"]
    assert conn.execute.call_count == 2
    assert "VIEWS IN" in str(conn.execute.call_args_list[0][0][0])
    assert "FROM 'BV'" in str(conn.execute.call_args_list[1][0][0])


def test_get_schema_names_paginates():
    results = [
        _result_with([["c", "S1"], ["c", "S2"]], _NAME_AT_1),
        _result_with([["c", "S3"]], _NAME_AT_1),
    ]
    dialect, conn = _dialect_with_results(results, page_size=2)
    names = dialect.get_schema_names(conn)
    assert names == ["s1", "s2", "s3"]
    assert conn.execute.call_count == 2
    assert "SCHEMAS" in str(conn.execute.call_args_list[0][0][0])


def test_get_sequence_names_paginates():
    results = [
        _result_with([["SEQ_A", "c"], ["SEQ_B", "c"]], _NAME_AT_0),
        _result_with([["SEQ_C", "c"]], _NAME_AT_0),
    ]
    dialect, conn = _dialect_with_results(results, page_size=2)
    names = dialect.get_sequence_names(conn, schema="myschema")
    assert names == ["seq_a", "seq_b", "seq_c"]
    assert conn.execute.call_count == 2
    assert "FROM 'SEQ_B'" in str(conn.execute.call_args_list[1][0][0])


def test_get_sequence_names_missing_schema_returns_empty():
    """The 2003 (schema does not exist) fallback is preserved through the helper."""
    from sqlalchemy import exc as sa_exc

    class _Orig(Exception):
        errno = 2003

    dialect = SnowflakeDialect()
    dialect._get_full_schema_name = MagicMock(return_value='"D"."NOPE"')
    conn = MagicMock()
    conn.execute.side_effect = sa_exc.ProgrammingError("SHOW SEQUENCES", {}, _Orig())
    assert dialect.get_sequence_names(conn, schema="nope") == []


def test_get_temp_table_names_paginates_and_filters():
    """Temp-table filtering by `kind` still works across paged SHOW TABLES."""
    pages = [
        [_row("REG1"), _row("TMP1", kind="TEMPORARY")],  # full
        [_row("TMP2", kind="TEMPORARY")],  # partial -> stop
    ]
    dialect, conn = _dialect_with_pages(pages, page_size=2)
    names = dialect.get_temp_table_names(conn, schema="myschema")
    assert names == ["tmp1", "tmp2"]
    assert conn.execute.call_count == 2
