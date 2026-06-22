#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Unit tests for the Snowflake string-escaping helpers in util.py.
# See tests/test_table_option_quoting.py for end-to-end option rendering coverage.
#
import pytest
from sqlalchemy.sql.sqltypes import String

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from snowflake.sqlalchemy.util import escape_backslashes, escape_string_literal_interior

# A single literal backslash. Spelled out to keep the parametrize tables
# readable — "\\" in source is one backslash at runtime.
BS = "\\"


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("", "", id="empty"),
        pytest.param("plain", "plain", id="no_special_chars"),
        pytest.param(BS, BS * 2, id="single_backslash"),
        pytest.param(BS * 2, BS * 4, id="double_backslash"),
        pytest.param("a" + BS + "b", "a" + BS * 2 + "b", id="embedded_backslash"),
        pytest.param(BS + "'", BS * 2 + "'", id="backslash_before_quote_only_bs"),
        pytest.param("100%", "100%", id="percent_untouched"),
        pytest.param("'", "'", id="lone_quote_untouched"),
        pytest.param("a" + BS + BS + "b", "a" + BS * 4 + "b", id="run_of_two"),
    ],
)
def test_escape_backslashes(value, expected):
    assert escape_backslashes(value) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("", "", id="empty"),
        pytest.param("plain", "plain", id="no_special_chars"),
        pytest.param("'", "''", id="single_quote_doubled"),
        pytest.param("''", "''''", id="two_quotes_doubled"),
        pytest.param(BS, BS * 2, id="backslash_doubled"),
        pytest.param("O'Brien", "O''Brien", id="apostrophe"),
        pytest.param("loc'; -- aaa", "loc''; -- aaa", id="quote_in_value"),
        # Combined backslash and quote: Snowflake (ESCAPE_STRING_LITERALS) reads
        # \' as an escaped quote; doubling the backslash neutralises it -> \\''
        pytest.param(
            "path" + BS + "' suffix",
            "path" + BS * 2 + "'' suffix",
            id="backslash_quote_escaped",
        ),
        # Percent signs must NOT be doubled (unlike SA's literal processor),
        # because the interior is interpolated into a %-formatted DDL template.
        pytest.param("a%b", "a%b", id="percent_not_doubled"),
        pytest.param("%s %d %%", "%s %d %%", id="format_specifiers_untouched"),
        # No surrounding quotes are added.
        pytest.param("x", "x", id="no_wrapping_quotes"),
    ],
)
def test_escape_string_literal_interior(value, expected):
    assert escape_string_literal_interior(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("a%b", id="percent"),
        pytest.param("100%", id="trailing_percent"),
        pytest.param("no percent here", id="no_percent"),
    ],
)
def test_interior_escape_diverges_from_sa_processor_on_percent(value):
    """Snowflake uses pyformat paramstyle, so the processor doubles '%' and
    wraps the value in quotes. Our DDL-option helper must do neither — this
    test fails loudly if anyone swaps the helper back to the processor.
    """
    dialect = SnowflakeDialect()
    assert dialect.identifier_preparer._double_percents is True
    processor = String()._cached_literal_processor(dialect)
    processed = processor(value)

    # Processor wraps in single-quotes; the helper does not.
    assert processed.startswith("'") and processed.endswith("'")
    helper = escape_string_literal_interior(value)
    assert not helper.startswith("'")

    if "%" in value:
        assert "%%" in processed
        assert "%%" not in helper
    else:
        # Absent percent signs, the processor interior matches the helper.
        assert processed[1:-1] == helper


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("plain", id="plain"),
        pytest.param("a'b", id="quote"),
        pytest.param(BS, id="backslash"),
        pytest.param("a%b'c" + BS + "d", id="mixed"),
    ],
)
def test_compiler_escape_methods_delegate_to_helper(value):
    dialect = SnowflakeDialect()
    ddl_compiler = dialect.ddl_compiler(dialect, None)
    sql_compiler = dialect.statement_compiler(dialect, None)
    expected = escape_string_literal_interior(value)
    assert ddl_compiler._escape_string_interior(value) == expected
    assert sql_compiler._escape_string_interior(value) == expected
