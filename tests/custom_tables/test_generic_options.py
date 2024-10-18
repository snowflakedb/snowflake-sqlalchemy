#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import pytest

from snowflake.sqlalchemy import (
    AsQueryOption,
    IdentifierOption,
    KeywordOption,
    LiteralOption,
    SnowflakeKeyword,
    TableOptionKey,
    TargetLagOption,
    exc,
)
from snowflake.sqlalchemy.sql.custom_schema.options.invalid_table_option import (
    InvalidTableOption,
)


def test_identifier_option():
    identifier = IdentifierOption.create(TableOptionKey.WAREHOUSE, "xsmall")
    assert identifier.render_option(None) == "WAREHOUSE = xsmall"


def test_literal_option():
    literal = LiteralOption.create(TableOptionKey.WAREHOUSE, "xsmall")
    assert literal.render_option(None) == "WAREHOUSE = 'xsmall'"


def test_identifier_option_without_name(snapshot):
    identifier = IdentifierOption("xsmall")
    with pytest.raises(exc.OptionKeyNotProvidedError) as exc_info:
        identifier.render_option(None)
    assert exc_info.value == snapshot


def test_identifier_option_with_wrong_type(snapshot):
    identifier = IdentifierOption.create(TableOptionKey.WAREHOUSE, 23)
    with pytest.raises(exc.InvalidTableParameterTypeError) as exc_info:
        identifier.render_option(None)
    assert exc_info.value == snapshot


def test_literal_option_with_wrong_type(snapshot):
    literal = LiteralOption.create(
        TableOptionKey.WAREHOUSE, SnowflakeKeyword.DOWNSTREAM
    )
    with pytest.raises(exc.InvalidTableParameterTypeError) as exc_info:
        literal.render_option(None)
    assert exc_info.value == snapshot


def test_invalid_as_query_option(snapshot):
    as_query = AsQueryOption.create(23)
    with pytest.raises(exc.InvalidTableParameterTypeError) as exc_info:
        as_query.render_option(None)
    assert exc_info.value == snapshot


@pytest.mark.parametrize(
    "table_option",
    [
        IdentifierOption,
        LiteralOption,
        KeywordOption,
    ],
)
def test_generic_option_with_wrong_type(table_option):
    literal = table_option.create(TableOptionKey.WAREHOUSE, 0.32)
    assert isinstance(literal, InvalidTableOption), "Expected InvalidTableOption"


@pytest.mark.parametrize(
    "table_option",
    [
        TargetLagOption,
        AsQueryOption,
    ],
)
def test_non_generic_option_with_wrong_type(table_option):
    literal = table_option.create(0.32)
    assert isinstance(literal, InvalidTableOption), "Expected InvalidTableOption"
