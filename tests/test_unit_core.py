#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest
from sqlalchemy.engine.url import URL

from snowflake.sqlalchemy import base
from snowflake.sqlalchemy.compat import IS_VERSION_20
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
# _is_single_table_reflection (SNOW-689531)
# ---------------------------------------------------------------------------


class TestIsSingleTableReflection:
    """Unit tests for the _is_single_table_reflection heuristic."""

    @pytest.mark.skipif(not IS_VERSION_20, reason="SA 2.x only")
    def test_sa2_returns_true_without_opt_in(self):
        """SA 2.x: singular calls are always single-table; no flag required."""
        assert _make_dialect()._is_single_table_reflection("PUBLIC") is True

    @pytest.mark.skipif(not IS_VERSION_20, reason="SA 2.x only")
    def test_sa2_returns_true_with_info_cache_present(self):
        """SA 2.x: info_cache presence is irrelevant — get_multi_* handles bulk."""
        assert (
            _make_dialect()._is_single_table_reflection("PUBLIC", info_cache={}) is True
        )

    @mock.patch("snowflake.sqlalchemy.snowdialect.IS_VERSION_20", False)
    def test_sa14_returns_false_without_opt_in(self):
        """SA 1.4: defaults to schema-wide queries for backward compatibility."""
        assert _make_dialect()._is_single_table_reflection("PUBLIC") is False

    @mock.patch("snowflake.sqlalchemy.snowdialect.IS_VERSION_20", False)
    def test_sa14_returns_true_with_opt_in_no_info_cache(self):
        """SA 1.4 + cache_column_metadata=True + no info_cache → single-table path."""
        dialect = _make_dialect(_cache_column_metadata=True)
        assert dialect._is_single_table_reflection("PUBLIC", info_cache=None) is True

    @mock.patch("snowflake.sqlalchemy.snowdialect.IS_VERSION_20", False)
    def test_sa14_returns_false_when_metadata_reflect_in_progress(self):
        """SA 1.4 + MetaData.reflect() in progress → fall back to schema-wide query."""
        dialect = _make_dialect(_cache_column_metadata=True)
        tables_info_key = (dialect._get_schema_tables_info.__name__, ("PUBLIC",), ())
        assert (
            dialect._is_single_table_reflection(
                "PUBLIC", info_cache={tables_info_key: {}}
            )
            is False
        )

    @mock.patch("snowflake.sqlalchemy.snowdialect.IS_VERSION_20", False)
    def test_sa14_returns_true_when_schema_tables_not_yet_cached(self):
        """SA 1.4 + cache_column_metadata=True + empty cache → single-table path."""
        dialect = _make_dialect(_cache_column_metadata=True)
        assert dialect._is_single_table_reflection("PUBLIC", info_cache={}) is True


# ---------------------------------------------------------------------------
# Single-table dispatch on SA 2.x (SNOW-689531)
# ---------------------------------------------------------------------------


class TestSingleTableDispatchSA2:
    """SA 2.x: singular reflection methods must route to table-specific queries
    without requiring cache_column_metadata=True."""

    @pytest.mark.skipif(not IS_VERSION_20, reason="SA 2.x only")
    def test_get_pk_constraint_uses_table_path(self):
        dialect = _make_dialect()
        connection = mock.Mock()
        expected = {"constrained_columns": ["id"], "name": "pk_foo"}

        with mock.patch.object(
            dialect, "_get_table_primary_keys", return_value=expected
        ) as tbl_mock, mock.patch.object(
            dialect,
            "_get_schema_primary_keys",
            side_effect=AssertionError("schema-wide PK query must not be called"),
        ) as schema_mock:
            result = dialect.get_pk_constraint(connection, "foo", schema="PUBLIC")

        tbl_mock.assert_called_once()
        schema_mock.assert_not_called()
        assert result == expected

    @pytest.mark.skipif(not IS_VERSION_20, reason="SA 2.x only")
    def test_get_unique_constraints_uses_table_path(self):
        dialect = _make_dialect()
        connection = mock.Mock()
        expected = [{"column_names": ["email"], "name": "uq_email"}]

        with mock.patch.object(
            dialect, "_get_table_unique_constraints", return_value=expected
        ) as tbl_mock, mock.patch.object(
            dialect,
            "_get_schema_unique_constraints",
            side_effect=AssertionError("schema-wide UK query must not be called"),
        ):
            result = dialect.get_unique_constraints(connection, "foo", schema="PUBLIC")

        tbl_mock.assert_called_once()
        assert result == expected

    @pytest.mark.skipif(not IS_VERSION_20, reason="SA 2.x only")
    def test_get_foreign_keys_uses_table_path(self):
        dialect = _make_dialect()
        connection = mock.Mock()
        expected = [{"referred_table": "bar", "constrained_columns": ["bar_id"]}]

        with mock.patch.object(
            dialect, "_get_table_foreign_keys", return_value=expected
        ) as tbl_mock, mock.patch.object(
            dialect,
            "_get_schema_foreign_keys",
            side_effect=AssertionError("schema-wide FK query must not be called"),
        ):
            result = dialect.get_foreign_keys(connection, "foo", schema="PUBLIC")

        tbl_mock.assert_called_once()
        assert result == expected

    @pytest.mark.skipif(not IS_VERSION_20, reason="SA 2.x only")
    def test_get_indexes_uses_table_path(self):
        dialect = _make_dialect()
        connection = mock.Mock()
        expected = [{"name": "idx_foo", "column_names": ["col1"], "unique": False}]

        with mock.patch.object(
            dialect, "_get_table_indexes", return_value=expected
        ) as tbl_mock, mock.patch.object(
            dialect,
            "get_multi_indexes",
            side_effect=AssertionError("schema-wide index query must not be called"),
        ):
            result = dialect.get_indexes(connection, "foo", schema="PUBLIC")

        tbl_mock.assert_called_once()
        assert result == expected
