#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import pytest
from sqlalchemy.sql.elements import quoted_name

from snowflake.sqlalchemy.base import SnowflakeIdentifierPreparer
from snowflake.sqlalchemy.name_utils import _NameUtils


class _DummyDialect:
    name = "snowflake"
    max_identifier_length = 255


@pytest.fixture(scope="module")
def name_utils():
    preparer = SnowflakeIdentifierPreparer(_DummyDialect())
    return _NameUtils(preparer)


def test_normalize_name_respects_none_and_empty(name_utils):
    assert name_utils.normalize_name(None) is None
    assert name_utils.normalize_name("") == ""


def test_normalize_name_quotes_when_lowercase_present(name_utils):
    result = name_utils.normalize_name("MySchema")
    assert isinstance(result, quoted_name)
    assert result.quote
    assert str(result) == "MySchema"


def test_normalize_name_returns_unquoted_uppercase(name_utils):
    assert name_utils.normalize_name("FOO") == "FOO"


def test_normalize_name_uses_identifier_preparer_rules(name_utils):
    result = name_utils.normalize_name("SELECT")
    assert isinstance(result, quoted_name)
    assert result.quote
    assert str(result) == "SELECT"
