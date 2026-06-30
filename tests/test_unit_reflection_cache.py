#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""
Unit tests for the targeted-query cache optimisation (SNOW-3720548 / GH-701).

Before the fix, reflecting a single table caused:
  - a full information_schema.columns scan across the entire schema, and
  - a DESC TABLE call for every structured-type table in the schema.

After the fix:
  - get_multi_columns with filter_names + cold cache issues a narrow
    WHERE table_name IN (...) query instead of a full schema scan.
  - get_multi_columns with filter_names + warm full-schema cache skips SQL
    entirely, reading from the cached result.
  - A filtered result is stored under a distinct cache key and never pollutes
    the full-schema cache entry.
  - _get_schema_columns dispatches to the right underlying query method based
    on whether filter_names is present in **kw.
  - DESC TABLE fan-out is eliminated implicitly: the targeted SQL only returns
    rows for the requested tables, so _build_column_info (which triggers DESC
    TABLE for structured-type columns) only processes those rows.
"""

from __future__ import annotations

import re
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

_DB = "MYDB"
_SCHEMA = "myschema"
_FULL_SCHEMA = '"MYDB"."MYSCHEMA"'

# @reflection.cache key format: (fn_name, positional_str_args_tuple, sorted_kw_items_tuple)
# info_cache and unreflectable are excluded by the decorator.
_FULL_SCHEMA_CACHE_KEY = ("_get_schema_columns", (_SCHEMA,), ())


@pytest.fixture
def dialect_no_db():
    """SnowflakeDialect without a live database connection."""
    return SnowflakeDialect()


def _make_mock_conn():
    """Return (conn, sql_log) where sql_log records information_schema queries.

    The mock connection returns plausible stubs for housekeeping queries
    (CURRENT_DATABASE, PRIMARY KEYS) and logs every other query for assertions.
    """
    conn = MagicMock()
    sql_log: list[dict] = []

    def _execute(stmt, params=None):
        sql = stmt.text if hasattr(stmt, "text") else str(stmt)
        r = MagicMock()
        if "current_database" in sql.lower():
            r.fetchone.return_value = (_DB, "MYSCHEMA")
        elif "PRIMARY KEYS" in sql:
            r.__iter__ = lambda s: iter([])
        else:
            sql_log.append({"sql": sql, "params": dict(params or {})})
            r.__iter__ = lambda s: iter([])
        return r

    conn.execute.side_effect = _execute
    return conn, sql_log


@contextmanager
def patch_column_query_methods(dialect, filtered_rows=(), all_rows=()):
    """Patch the query methods that _get_schema_columns dispatches to.

    Stubs out schema-resolution helpers and replaces the two column-query
    methods with MagicMocks so tests can assert on dispatch logic without
    hitting a real database.

    Yields (mock_all_columns_query, mock_targeted_query, mock_primary_keys).
    """
    mock_all_columns_query = MagicMock(return_value=iter(all_rows))
    mock_targeted_query = MagicMock(return_value=iter(filtered_rows))
    mock_primary_keys = MagicMock(return_value={})
    with patch.object(
        dialect, "_get_full_schema_name", return_value=_FULL_SCHEMA
    ), patch.object(
        dialect, "_current_database_schema", return_value=(_DB, "MYSCHEMA")
    ), patch.object(
        dialect, "_get_schema_primary_keys", mock_primary_keys
    ), patch.object(
        dialect, "_query_all_columns_info", mock_all_columns_query
    ), patch.object(
        dialect, "_query_filtered_columns_info", mock_targeted_query
    ):
        yield mock_all_columns_query, mock_targeted_query, mock_primary_keys


def test_targeted_query_empty_filter_names_returns_empty_without_sql(dialect_no_db):
    """Empty filter_names must short-circuit before SQL — AND ic.table_name IN () is invalid."""
    conn, sql_log = _make_mock_conn()

    result = dialect_no_db._query_filtered_columns_info(conn, _FULL_SCHEMA, ())

    assert list(result) == []
    assert sql_log == [], "no SQL should be issued for empty filter_names"


def test_targeted_query_contains_table_name_in_clause(dialect_no_db):
    """Targeted query must have AND ic.table_name IN (...) in its WHERE clause."""
    conn, sql_log = _make_mock_conn()

    dialect_no_db._query_filtered_columns_info(
        conn, _FULL_SCHEMA, ("TABLE_A", "TABLE_B")
    )

    assert len(sql_log) == 1, "expected exactly one SQL statement"
    assert "AND ic.table_name IN" in sql_log[0]["sql"]


@pytest.mark.parametrize(
    "filter_names, expected_table_params",
    [
        pytest.param(("ONLY_TABLE",), {"t0": "ONLY_TABLE"}, id="single_table"),
        pytest.param(
            ("A", "B", "C"), {"t0": "A", "t1": "B", "t2": "C"}, id="three_tables"
        ),
    ],
)
def test_targeted_query_placeholder_count(
    dialect_no_db, filter_names, expected_table_params
):
    """Each table in filter_names gets its own numbered bind-parameter placeholder."""
    conn, sql_log = _make_mock_conn()

    dialect_no_db._query_filtered_columns_info(conn, _FULL_SCHEMA, filter_names)

    params = sql_log[0]["params"]
    table_params = {
        k: v for k, v in params.items() if k.startswith("t") and k[1:].isdigit()
    }
    assert table_params == expected_table_params
    assert len(re.findall(r":t\d+", sql_log[0]["sql"])) == len(expected_table_params)


def test_targeted_query_params_contain_denormalized_table_names(dialect_no_db):
    """Table names are denormalized (uppercased) before binding to params."""
    conn, sql_log = _make_mock_conn()

    dialect_no_db._query_filtered_columns_info(conn, _FULL_SCHEMA, ("my_table",))

    params = sql_log[0]["params"]
    table_values = {v for k, v in params.items() if k.startswith("t")}
    assert "MY_TABLE" in table_values


def test_targeted_query_table_schema_param_is_present(dialect_no_db):
    """table_schema bind param must be set for the WHERE table_schema filter."""
    conn, sql_log = _make_mock_conn()

    dialect_no_db._query_filtered_columns_info(conn, _FULL_SCHEMA, ("TABLE_A",))

    assert "table_schema" in sql_log[0]["params"]


def test_targeted_query_selects_same_columns_as_full_query(dialect_no_db):
    """The SELECT column list must be identical to _query_all_columns_info."""
    conn, sql_log = _make_mock_conn()

    dialect_no_db._query_filtered_columns_info(conn, _FULL_SCHEMA, ("TABLE_A",))

    sql = sql_log[0]["sql"]
    for col in (
        "ic.table_name",
        "ic.column_name",
        "ic.data_type",
        "ic.character_maximum_length",
        "ic.numeric_precision",
        "ic.numeric_scale",
        "ic.is_nullable",
        "ic.column_default",
        "ic.is_identity",
        "ic.comment",
        "ic.identity_start",
        "ic.identity_increment",
        "ic.identity_generation",
        "ic.identity_cycle",
        "ic.identity_ordered",
        "ic.data_type_alias",
    ):
        assert col in sql, f"expected column {col!r} in SELECT list; got:\n{sql}"


def test_schema_columns_without_filter_names_calls_all_columns_query(dialect_no_db):
    """No filter_names → _query_all_columns_info is called, not the targeted variant."""
    conn = MagicMock()

    with patch_column_query_methods(dialect_no_db) as (
        mock_all_columns_query,
        mock_targeted_query,
        _,
    ):
        dialect_no_db._get_schema_columns(conn, _SCHEMA, info_cache={})

    mock_all_columns_query.assert_called_once()
    mock_targeted_query.assert_not_called()


def test_schema_columns_with_filter_names_calls_targeted_query(dialect_no_db):
    """filter_names kwarg → _query_filtered_columns_info is called, not the full variant."""
    conn = MagicMock()

    with patch_column_query_methods(dialect_no_db) as (
        mock_all_columns_query,
        mock_targeted_query,
        _,
    ):
        dialect_no_db._get_schema_columns(
            conn, _SCHEMA, filter_names=("TABLE_A",), info_cache={}
        )

    mock_targeted_query.assert_called_once_with(conn, _FULL_SCHEMA, ("TABLE_A",))
    mock_all_columns_query.assert_not_called()


def test_schema_columns_filter_names_not_forwarded_to_all_columns_query(dialect_no_db):
    """filter_names must be consumed and not bleed into _query_all_columns_info's
    call — that would contaminate its @reflection.cache key."""
    conn = MagicMock()

    with patch_column_query_methods(dialect_no_db) as (mock_all_columns_query, _, _):
        dialect_no_db._get_schema_columns(conn, _SCHEMA, info_cache={})

    _, call_kw = mock_all_columns_query.call_args
    assert "filter_names" not in call_kw


def test_schema_columns_filter_names_not_forwarded_to_primary_keys_query(dialect_no_db):
    """filter_names must not appear in the _get_schema_primary_keys call so that
    PK results are shared across full-schema and targeted reflection paths."""
    conn = MagicMock()

    with patch_column_query_methods(dialect_no_db) as (_, _, mock_primary_keys):
        dialect_no_db._get_schema_columns(
            conn, _SCHEMA, filter_names=("TABLE_A",), info_cache={}
        )

    _, pk_kw = mock_primary_keys.call_args
    assert "filter_names" not in pk_kw


def test_schema_columns_filter_names_creates_distinct_cache_entry(dialect_no_db):
    """A targeted call must be stored under a key that includes filter_names,
    separate from the full-schema key, so they can coexist in info_cache."""
    conn = MagicMock()
    info_cache: dict = {}

    with patch_column_query_methods(dialect_no_db):
        dialect_no_db._get_schema_columns(
            conn, _SCHEMA, filter_names=("TABLE_A",), info_cache=info_cache
        )

    filtered_key = (
        "_get_schema_columns",
        (_SCHEMA,),
        (("filter_names", ("TABLE_A",)),),
    )
    assert (
        filtered_key in info_cache
    ), "filtered result should be cached under its own key"
    assert (
        _FULL_SCHEMA_CACHE_KEY not in info_cache
    ), "filtered result must not be stored under the full-schema key"


def test_schema_columns_full_schema_result_cached_under_schema_only_key(dialect_no_db):
    """Full-schema call must be stored under the plain (schema,) key."""
    conn = MagicMock()
    info_cache: dict = {}

    with patch_column_query_methods(dialect_no_db):
        dialect_no_db._get_schema_columns(conn, _SCHEMA, info_cache=info_cache)

    assert _FULL_SCHEMA_CACHE_KEY in info_cache


def test_multi_columns_warm_cache_skips_sql(dialect_no_db):
    """Warm full-schema cache: connection.execute must never be called."""
    conn = MagicMock()
    info_cache = {_FULL_SCHEMA_CACHE_KEY: {"mytable": [{"name": "col1"}]}}

    dialect_no_db.get_multi_columns(
        conn,
        schema=_SCHEMA,
        filter_names=["mytable"],
        info_cache=info_cache,
    )

    conn.execute.assert_not_called()


@pytest.mark.parametrize(
    "filter_names, expected_keys",
    [
        pytest.param(["table_a"], {(_SCHEMA, "table_a")}, id="single_table"),
        pytest.param(
            ["t1", "t3"], {(_SCHEMA, "t1"), (_SCHEMA, "t3")}, id="multi_table"
        ),
    ],
)
def test_multi_columns_warm_cache_serves_correct_subset(
    dialect_no_db, filter_names, expected_keys
):
    """Warm cache: only the requested tables are returned, no SQL issued."""
    conn = MagicMock()
    info_cache = {
        _FULL_SCHEMA_CACHE_KEY: {
            "table_a": [{"name": "id"}],
            "table_b": [{"name": "name"}],
            "t1": [{"name": "a"}],
            "t2": [{"name": "b"}],
            "t3": [{"name": "c"}],
        },
    }

    result = dialect_no_db.get_multi_columns(
        conn, schema=_SCHEMA, filter_names=filter_names, info_cache=info_cache
    )

    assert {r[0] for r in result} == expected_keys
    conn.execute.assert_not_called()


def test_multi_columns_cold_cache_passes_filter_names_to_schema_columns(dialect_no_db):
    """Cold cache + filter_names → _get_schema_columns is called with filter_names kwarg."""
    conn = MagicMock()
    info_cache: dict = {}
    captured: list[dict] = []

    def fake_schema_columns(connection, schema, **kw):
        captured.append({"schema": schema, "kw": dict(kw)})
        fn = kw.get("filter_names")
        return {n: [] for n in fn} if fn else {}

    with patch.object(dialect_no_db, "_get_schema_columns", fake_schema_columns):
        dialect_no_db.get_multi_columns(
            conn,
            schema=_SCHEMA,
            filter_names=["mytable"],
            info_cache=info_cache,
        )

    assert len(captured) == 1
    assert captured[0]["kw"].get("filter_names") == ("mytable",)


def test_multi_columns_cold_cache_filter_names_normalised_to_tuple(dialect_no_db):
    """filter_names is converted to a tuple before passing to _get_schema_columns
    so the @reflection.cache key is stable regardless of the input container type."""
    conn = MagicMock()
    captured: list = []

    def fake_schema_columns(connection, schema, **kw):
        captured.append(kw.get("filter_names"))
        return {}

    with patch.object(dialect_no_db, "_get_schema_columns", fake_schema_columns):
        dialect_no_db.get_multi_columns(
            conn,
            schema=_SCHEMA,
            filter_names=["a", "b"],
            info_cache={},
        )

    assert isinstance(
        captured[0], tuple
    ), "filter_names must be a tuple for stable cache keys"


def test_multi_columns_cold_cache_no_filter_names_calls_full_schema_path(dialect_no_db):
    """Cold cache + no filter_names → _get_schema_columns called without filter_names."""
    conn = MagicMock()
    captured: list[dict] = []

    def fake_schema_columns(connection, schema, **kw):
        captured.append(dict(kw))
        return {}

    with patch.object(dialect_no_db, "_get_schema_columns", fake_schema_columns):
        dialect_no_db.get_multi_columns(conn, schema=_SCHEMA, info_cache={})

    assert len(captured) == 1
    assert "filter_names" not in captured[0]


def test_multi_columns_filtered_result_not_stored_under_full_schema_key(dialect_no_db):
    """Targeted result must never populate the full-schema cache key.

    If it did, a later MetaData.reflect() (no filter_names) would get a cache
    hit and silently return only the subset that was previously reflected.
    """
    conn = MagicMock()
    info_cache: dict = {}

    def fake_schema_columns(connection, schema, **kw):
        return {"mytable": []}

    with patch.object(dialect_no_db, "_get_schema_columns", fake_schema_columns):
        dialect_no_db.get_multi_columns(
            conn,
            schema=_SCHEMA,
            filter_names=["mytable"],
            info_cache=info_cache,
        )

    assert _FULL_SCHEMA_CACHE_KEY not in info_cache, (
        "Filtered result was written to the full-schema cache key — "
        "a later MetaData.reflect() would get a stale partial result."
    )


def test_multi_columns_warm_cache_takes_priority_over_targeted_path(dialect_no_db):
    """Warm full-schema cache must be used even when filter_names is set, and
    _get_schema_columns must not be called at all."""
    conn = MagicMock()
    info_cache = {_FULL_SCHEMA_CACHE_KEY: {"mytable": [{"name": "id"}]}}
    schema_columns_calls: list = []

    def fake_schema_columns(connection, schema, **kw):
        schema_columns_calls.append(kw)
        return {}

    with patch.object(dialect_no_db, "_get_schema_columns", fake_schema_columns):
        dialect_no_db.get_multi_columns(
            conn,
            schema=_SCHEMA,
            filter_names=["mytable"],
            info_cache=info_cache,
        )

    assert (
        schema_columns_calls == []
    ), "_get_schema_columns was called despite a warm full-schema cache entry"
