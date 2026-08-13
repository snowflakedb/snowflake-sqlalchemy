#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from unittest.mock import MagicMock, patch

import pytest

from snowflake.sqlalchemy.parser.custom_type_parser import parse_index_columns
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


def _show_indexes_result(rows):
    """A stub of the CursorResult that SHOW INDEXES produces, exposing just
    what _map_name_to_idx / _parse_index_rows consume: cursor.description
    (column name first per entry) and cursor.fetchall().
    """
    result = MagicMock()
    result.cursor.description = [
        (name,)
        for name in ("name", "table", "is_unique", "columns", "included_columns")
    ]
    result.cursor.fetchall.return_value = rows
    return result


_PK_SENTINEL_ROW = ("SYS_INDEX_MY_TABLE_PRIMARY", "MY_TABLE", "Y", "[ID]", "[]")
_PLAIN_INDEX_ROW = ("IX_MY_TABLE_VAL", "MY_TABLE", "N", "[VAL]", "[]")
_UNIQUE_INDEX_ROW = ("UQ_MY_TABLE_VAL", "MY_TABLE", "Y", "[VAL]", "[OTHER]")


@pytest.fixture
def dialect():
    """SnowflakeDialect without a live connection."""
    return SnowflakeDialect()


class TestParseIndexColumns:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("[]", []),
            ("[ ]", []),
            ("[VAL]", ["VAL"]),
            ("[A, B]", ["A", "B"]),
        ],
    )
    def test_parse(self, raw, expected):
        assert parse_index_columns(raw) == expected


class TestParseIndexRows:
    def test_index_name_is_normalized(self, dialect):
        parsed = dialect._parse_index_rows(_show_indexes_result([_PLAIN_INDEX_ROW]))
        (index,) = parsed["my_table"]
        assert index["name"] == "ix_my_table_val"

    def test_case_sensitive_index_name_is_preserved(self, dialect):
        row = ("MyIndex", "MY_TABLE", "N", "[VAL]", "[]")
        parsed = dialect._parse_index_rows(_show_indexes_result([row]))
        (index,) = parsed["my_table"]
        assert index["name"] == "MyIndex"

    def test_empty_include_columns(self, dialect):
        parsed = dialect._parse_index_rows(_show_indexes_result([_PLAIN_INDEX_ROW]))
        (index,) = parsed["my_table"]
        assert index["include_columns"] == []

    def test_columns_unique_flag_and_include_columns_normalized(self, dialect):
        parsed = dialect._parse_index_rows(_show_indexes_result([_UNIQUE_INDEX_ROW]))
        (index,) = parsed["my_table"]
        assert index["unique"] is True
        assert index["column_names"] == ["val"]
        assert index["include_columns"] == ["other"]

    def test_pk_sentinel_is_filtered(self, dialect):
        parsed = dialect._parse_index_rows(
            _show_indexes_result([_PK_SENTINEL_ROW, _PLAIN_INDEX_ROW])
        )
        assert len(parsed["my_table"]) == 1
        assert parsed["my_table"][0]["name"] != "sys_index_my_table_primary"


class TestGetMultiIndexesKeying:
    def _call(self, dialect, schema):
        connection = MagicMock()
        connection.execute.return_value = _show_indexes_result([_PLAIN_INDEX_ROW])
        with (
            patch.object(
                dialect, "get_table_names_with_prefix", return_value=["my_table"]
            ) as get_hybrid_names,
            patch.object(
                dialect, "_get_full_schema_name", return_value='"MYDB"."MYSCHEMA"'
            ) as get_full_name,
        ):
            result = dialect.get_multi_indexes(
                connection, schema=schema, filter_names=["my_table"]
            )
        return result, get_hybrid_names, get_full_name

    def test_default_schema_keys_are_none(self, dialect):
        dialect.default_schema_name = "myschema"
        result, get_hybrid_names, get_full_name = self._call(dialect, schema=None)

        # Must key on what was actually passed
        assert [key for key, _ in result] == [(None, "my_table")]

        # Internally, call to the default schema
        assert get_hybrid_names.call_args.kwargs["schema"] == "myschema"
        assert get_full_name.call_args.args[1] == "myschema"

    def test_explicit_schema_in_keys(self, dialect):
        result, _, _ = self._call(dialect, schema="myschema")
        assert [key for key, _ in result] == [("myschema", "my_table")]

    def test_no_hybrid_tables_short_circuits(self, dialect):
        dialect.default_schema_name = "myschema"
        connection = MagicMock()
        with patch.object(dialect, "get_table_names_with_prefix", return_value=[]):
            assert (
                dialect.get_multi_indexes(connection, schema=None, filter_names=["t"])
                == []
            )
        connection.execute.assert_not_called()
