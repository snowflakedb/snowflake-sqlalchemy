#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest
from sqlalchemy.engine.url import URL
from sqlalchemy.sql.elements import quoted_name

from snowflake.sqlalchemy import base
from snowflake.sqlalchemy import snowdialect as sd_module
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


def _make_dialect(**attrs):
    """Return a SnowflakeDialect instance without a live connection."""
    dialect = SnowflakeDialect()
    dialect.default_schema_name = "PUBLIC"
    for k, v in attrs.items():
        setattr(dialect, k, v)
    return dialect


def test_create_connect_args():
    sfdialect = base.dialect()

    test_data = [
        (
            # 0: full host name and no account
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount.snowflakecomputing.com",
                query={},
            ),
            {
                "autocommit": False,
                "host": "testaccount.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
            },
        ),
        (
            # 1: account name only
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount",
                query={},
            ),
            {
                "autocommit": False,
                "host": "testaccount.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
                "port": "443",
                "account": "testaccount",
            },
        ),
        (
            # 2: account name including region
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount.eu-central-1",
                query={},
            ),
            {
                "autocommit": False,
                "host": "testaccount.eu-central-1.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
                "port": "443",
                "account": "testaccount",
            },
        ),
        (
            # 3: full host including region
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount.eu-central-1.snowflakecomputing.com",
                query={},
            ),
            {
                "autocommit": False,
                "host": "testaccount.eu-central-1.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
            },
        ),
        (
            # 4: full host including region and account
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount.eu-central-1.snowflakecomputing.com",
                query={"account": "testaccount"},
            ),
            {
                "autocommit": False,
                "host": "testaccount.eu-central-1.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
                "account": "testaccount",
            },
        ),
        (
            # 5: full host including region and account including region
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount.eu-central-1.snowflakecomputing.com",
                query={"account": "testaccount.eu-central-1"},
            ),
            {
                "autocommit": False,
                "host": "testaccount.eu-central-1.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
                "account": "testaccount.eu-central-1",
            },
        ),
        (
            # 6: full host including region and account including region
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="snowflake.reg.local",
                port="8082",
                query={"account": "testaccount"},
            ),
            {
                "autocommit": False,
                "host": "snowflake.reg.local",
                "password": "testpassword",
                "user": "testuser",
                "port": 8082,
                "account": "testaccount",
            },
        ),
        (
            # 7: Global URL
            URL.create(
                "snowflake",
                username="testuser",
                password="testpassword",
                host="testaccount-hso894gsiuafdhsaj935.global",
            ),
            {
                "autocommit": False,
                "host": "testaccount-hso894gsiuafdhsaj935.global.snowflakecomputing.com",
                "password": "testpassword",
                "user": "testuser",
                "port": "443",
                "account": "testaccount",
            },
        ),
    ]

    for idx, ts in enumerate(test_data):
        _, opts = sfdialect.create_connect_args(ts[0])
        assert opts == ts[1], f"Failed: {idx}: {ts[0]}"


def test_normalize_name_empty_string():
    """normalize_name should handle empty strings without crashing.

    Reported as: SNOW-593204
    https://github.com/snowflakedb/snowflake-sqlalchemy/issues/296

    Calling normalize_name("") used to crash with IndexError because
    _requires_quotes tried to access value[0] on an empty string.
    """
    sfdialect = base.dialect()

    # Should not raise IndexError
    result = sfdialect.normalize_name("")
    assert result == ""

    # Also test None for completeness
    assert sfdialect.normalize_name(None) is None


def test_denormalize_quote_join():
    sfdialect = base.dialect()

    test_data = [
        (["abc", "cde"], "abc.cde"),
        (["abc.cde", "def"], "abc.cde.def"),
        (['"Abc".cde', "def"], '"Abc".cde.def'),
        (['"Abc".cde', '"dEf"'], '"Abc".cde."dEf"'),
    ]
    for ts in test_data:
        assert sfdialect._denormalize_quote_join(*ts[0]) == ts[1]


@pytest.mark.parametrize(
    "raw_value, expected",
    [
        pytest.param(("8.10.2",), (8, 10, 2), id="simple"),
        pytest.param(("9.11.3 20241110",), (9, 11, 3), id="with_additional_parts"),
        pytest.param(
            ("   9.11.3   20241110  ",), (9, 11, 3), id="with_additional_whitespace"
        ),
        pytest.param(("1.2.3", "4.5.6"), (1, 2, 3), id="multiple_columns"),
        pytest.param(None, None, id="no_row"),
        pytest.param((), None, id="empty_result"),
    ],
)
def test_get_server_version_info_parsing(raw_value, expected):
    sfdialect = base.dialect()

    connection = mock.Mock()
    cursor_result = mock.Mock()
    cursor_result.fetchone.return_value = raw_value
    connection.execute.return_value = cursor_result

    result = sfdialect._get_server_version_info(connection)
    if expected is None:
        assert result is None
    else:
        assert result == expected


# ---------------------------------------------------------------------------
# Single-table dispatch on SA 2.x (SNOW-689531)
# ---------------------------------------------------------------------------


class TestSingleTableDispatchSA2:
    """SA 2.x: singular reflection methods must route to table-specific queries."""

    @pytest.mark.parametrize(
        "public_method,table_method,schema_method,expected",
        [
            (
                "get_pk_constraint",
                "_get_table_primary_keys",
                "_get_schema_primary_keys",
                {"constrained_columns": ["id"], "name": "pk_foo"},
            ),
            (
                "get_unique_constraints",
                "_get_table_unique_constraints",
                "_get_schema_unique_constraints",
                [{"column_names": ["email"], "name": "uq_email"}],
            ),
            (
                "get_foreign_keys",
                "_get_table_foreign_keys",
                "_get_schema_foreign_keys",
                [{"referred_table": "bar", "constrained_columns": ["bar_id"]}],
            ),
            (
                "get_indexes",
                "_get_table_indexes",
                "get_multi_indexes",
                [{"name": "idx_foo", "column_names": ["col1"], "unique": False}],
            ),
        ],
    )
    def test_uses_table_path(
        self, public_method, table_method, schema_method, expected
    ):
        dialect = _make_dialect()
        connection = mock.Mock()

        with mock.patch.object(
            dialect, table_method, return_value=expected
        ) as tbl_mock, mock.patch.object(
            dialect,
            schema_method,
            side_effect=AssertionError(f"{schema_method} must not be called"),
        ):
            result = getattr(dialect, public_method)(connection, "foo", schema="PUBLIC")

        tbl_mock.assert_called_once()
        assert result == expected

    def test_get_indexes_passes_raw_tablename_not_normalized(self):
        """get_indexes must pass the raw tablename string to _get_table_indexes.

        normalize_name() converts plain lowercase strings to
        quoted_name(quote=True).  quoted_name.upper() is a deliberate no-op
        when quote=True, so if the normalized form reaches _always_quote_join
        the identifier stays lowercase and Snowflake treats it as a
        case-sensitive lookup — failing to find a table stored as UPPERCASE.
        """
        dialect = _make_dialect()
        connection = mock.Mock()
        received = {}

        def capture(conn, table_name, schema, **kw):
            received["table_name"] = table_name
            return []

        with mock.patch.object(dialect, "_get_table_indexes", side_effect=capture):
            dialect.get_indexes(connection, "my_table", schema="PUBLIC")

        # Must be a plain str, not a quoted_name, so denormalize_name can
        # uppercase it correctly inside _always_quote_join.
        assert received["table_name"] == "my_table"
        assert type(received["table_name"]) is str

    def test_get_columns_quoted_name_with_embedded_dot_is_atomic(self):
        """quoted_name with embedded dot must not be re-split into extra schema parts."""
        dialect = _make_dialect()
        connection = mock.Mock()
        received = {}

        with mock.patch.object(sd_module, "_StructuredTypeInfoManager") as MockMgr:
            mock_instance = mock.Mock()
            mock_instance.get_table_columns_by_full_name.return_value = {}
            MockMgr.return_value = mock_instance

            dialect.get_columns(
                connection,
                quoted_name("weird.name", True),
                schema="PUBLIC",
            )

            call_args = mock_instance.get_table_columns_by_full_name.call_args
            received["full"] = call_args[0][0]

        full = received["full"]
        in_quote, parts = False, 0
        for ch in full:
            if ch == '"':
                in_quote = not in_quote
            elif ch == "." and not in_quote:
                parts += 1
        assert (
            parts == 1
        ), f"Expected 2-part reference (schema.table), got {parts + 1} parts: {full!r}"
        assert (
            '"weird.name"' in full
        ), f"Expected quoted atomic identifier '\"weird.name\"' in {full!r}"

    def test_get_columns_dotted_plain_string_uses_last_component(self):
        """A plain 'schema.table' string takes the last component as the table name."""
        dialect = _make_dialect()
        connection = mock.Mock()
        received = {}

        with mock.patch.object(sd_module, "_StructuredTypeInfoManager") as MockMgr:
            mock_instance = mock.Mock()
            mock_instance.get_table_columns_by_full_name.return_value = {}
            MockMgr.return_value = mock_instance

            dialect.get_columns(connection, "myschema.mytable", schema="PUBLIC")

            call_args = mock_instance.get_table_columns_by_full_name.call_args
            received["full"] = call_args[0][0]

        full = received["full"]
        assert '"MYTABLE"' in full, f"Expected last component 'MYTABLE' in {full!r}"
        assert (
            '"MYSCHEMA"' not in full
        ), f"Schema component from table_name must be dropped; got {full!r}"


def _capture_view_sql(dialect, view_name, schema="PUBLIC"):
    """Call get_view_definition and return the SQL string passed to execute()."""
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.keys.return_value = []
    cursor.fetchone.return_value = None
    conn.execute.return_value = cursor
    try:
        dialect.get_view_definition(conn, view_name, schema=schema)
    except Exception:
        pass
    assert conn.execute.called
    sql_arg = conn.execute.call_args[0][0]
    return sql_arg.text if hasattr(sql_arg, "text") else str(sql_arg)


class TestGetViewDefinitionLikeEscaping:
    """get_view_definition must escape single quotes in the LIKE value."""

    def test_plain_view_name_produces_valid_sql(self):
        """A normal view name must appear correctly in the LIKE clause."""
        d = _make_dialect()
        sql = _capture_view_sql(d, "my_view")
        assert "LIKE" in sql
        assert "MY_VIEW" in sql or "my_view" in sql

    @pytest.mark.parametrize(
        "view_name, expected, not_expected",
        [
            pytest.param("o'brien", ["''"], ['"o'], id="simple_quote"),
            pytest.param("back\\slash", ["\\\\"], [], id="backslash_doubled"),
        ],
    )
    def test_special_chars_are_escaped(self, view_name, expected, not_expected):
        sql = _capture_view_sql(_make_dialect(), view_name)
        for fragment in expected:
            assert (
                fragment in sql
            ), f"Expected {fragment!r} in SQL for {view_name!r}, got: {sql!r}"
        for fragment in not_expected:
            assert (
                fragment not in sql
            ), f"Unexpected {fragment!r} in SQL for {view_name!r}, got: {sql!r}"


def _capture_comment_sql(dialect, method_name, table_name, schema="PUBLIC"):
    """Return the SQL string _get_table_comment/_get_view_comment passes to execute()."""
    conn = mock.MagicMock()
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = None
    conn.execute.return_value = cursor
    with mock.patch.object(
        dialect,
        "_current_database_schema",
        return_value=("CURRENT_DB", "CURRENT_SCHEMA"),
    ):
        getattr(dialect, method_name)(conn, table_name, schema=schema)
    assert conn.execute.called
    sql_arg = conn.execute.call_args[0][0]
    return sql_arg.text if hasattr(sql_arg, "text") else str(sql_arg)


class TestReflectionCommentLikeEscaping:
    """SNOW-3649853 residual: _get_table_comment / _get_view_comment build a
    ``SHOW ... LIKE '{table_name}'`` literal, so the interpolated value must be
    single-quote escaped exactly like get_view_definition (snowdialect.py:1526).
    """

    @pytest.mark.parametrize("method_name", ["_get_table_comment", "_get_view_comment"])
    def test_plain_name_unchanged_bcr(self, method_name):
        # Behaviour-preserving: a quote-free name is untouched (no denormalize,
        # no over-quoting) — keeps the cross-database-reflection contract.
        sql = _capture_comment_sql(_make_dialect(), method_name, "my_table")
        assert "LIKE 'my_table'" in sql
        assert "''" not in sql

    @pytest.mark.parametrize("method_name", ["_get_table_comment", "_get_view_comment"])
    @pytest.mark.parametrize(
        "table_name, expected, not_expected",
        [
            pytest.param("o'brien", ["''"], ['"o'], id="simple_quote"),
            pytest.param("back\\slash", ["\\\\"], [], id="backslash_doubled"),
        ],
    )
    def test_special_chars_escaped(
        self, method_name, table_name, expected, not_expected
    ):
        sql = _capture_comment_sql(_make_dialect(), method_name, table_name)
        for fragment in expected:
            assert (
                fragment in sql
            ), f"Expected {fragment!r} in SQL for {table_name!r}, got: {sql!r}"
        for fragment in not_expected:
            assert (
                fragment not in sql
            ), f"Unexpected {fragment!r} in SQL for {table_name!r}, got: {sql!r}"
