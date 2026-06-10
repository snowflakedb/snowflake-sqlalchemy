#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock

import pytest
from sqlalchemy.engine.url import URL

from snowflake.sqlalchemy import base
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
