#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""
Unit tests for _NameUtils._quote_component and _NameUtils.always_quote_join.

These methods are the canonical always-quote helpers shared by
SnowflakeDialect._always_quote_join, _get_full_schema_name, and
_StructuredTypeInfoManager.get_table_columns.  Correctness here guarantees
all three callers stay consistent automatically.
"""

import re

import pytest
from sqlalchemy.sql.elements import quoted_name

from snowflake.sqlalchemy.name_utils import _NameUtils
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


@pytest.fixture
def nu():
    return _NameUtils(SnowflakeDialect().identifier_preparer)


# ---------------------------------------------------------------------------
# _quote_component
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "component, expected",
    [
        pytest.param(
            quoted_name("myschema", None), '"MYSCHEMA"', id="lowercase→uppercase"
        ),
        pytest.param(quoted_name("MYSCHEMA", None), '"MYSCHEMA"', id="uppercase"),
        pytest.param(quoted_name("MySchema", None), '"MySchema"', id="mixed-case"),
        pytest.param(
            quoted_name("myschema", True),
            '"myschema"',
            id="quote=True preserves lowercase",
        ),
        pytest.param(
            quoted_name("MySchema", True),
            '"MySchema"',
            id="quote=True preserves mixed-case",
        ),
    ],
)
def test_quote_component(nu, component, expected):
    assert nu._quote_component(component) == expected


def test_quote_component_escapes_internal_double_quote(nu):
    result = nu._quote_component(quoted_name('my"schema', None))
    assert result.startswith('"') and result.endswith('"')
    assert '""' in result  # SQL double-quote escaping


# ---------------------------------------------------------------------------
# always_quote_join
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "idents, expected",
    [
        # plain identifiers
        pytest.param(
            ("myschema", "mytable"), '"MYSCHEMA"."MYTABLE"', id="plain lowercase"
        ),
        pytest.param(
            ("MYSCHEMA", "MYTABLE"), '"MYSCHEMA"."MYTABLE"', id="plain uppercase"
        ),
        pytest.param(("MySchema", "MyTable"), '"MySchema"."MyTable"', id="mixed case"),
        pytest.param(
            (None, "myschema", "mytable"), '"MYSCHEMA"."MYTABLE"', id="None skipped"
        ),
        pytest.param(("myschema",), '"MYSCHEMA"', id="single ident"),
        # database-qualified schemas — 1.10.1 regression guard
        pytest.param(
            ("MYDB.MYSCHEMA", "mytable"),
            '"MYDB"."MYSCHEMA"."MYTABLE"',
            id="unquoted db-qualified schema",
        ),
        pytest.param(
            ("mydb.myschema", "mytable"),
            '"MYDB"."MYSCHEMA"."MYTABLE"',
            id="lowercase db-qualified schema",
        ),
        pytest.param(
            ('"MYDB"."MYSCHEMA"', "mytable"),
            '"MYDB"."MYSCHEMA"."MYTABLE"',
            id="pre-quoted db-qualified schema",
        ),
        pytest.param(
            ('"my.schema"', "mytable"),
            '"my.schema"."MYTABLE"',
            id="literal dot in quoted component not split",
        ),
        pytest.param(
            ("myschema", "foo.bar"),
            '"MYSCHEMA"."FOO"."BAR"',
            id="dot in table splits into components",
        ),
        # case-sensitivity signal (quote=True) preserved
        pytest.param(
            (quoted_name("myschema", True), "mytable"),
            '"myschema"."MYTABLE"',
            id="quote=True lowercase case preserved",
        ),
        pytest.param(
            (quoted_name("MySchema", True), "mytable"),
            '"MySchema"."MYTABLE"',
            id="quote=True mixed-case preserved",
        ),
    ],
)
def test_always_quote_join(nu, idents, expected):
    assert nu.always_quote_join(*idents) == expected
    assert '"""' not in nu.always_quote_join(*idents)


# ---------------------------------------------------------------------------
# always_quote_join — identifier quoting correctness
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "idents, blocked",
    [
        pytest.param(("myschema; -- aaa", "mytable"), ";", id="semicolon in schema"),
        pytest.param(("myschema", "mytable; -- aaa"), ";", id="semicolon in table"),
        pytest.param(("x'; -- aaa", "t"), ";", id="quote and semicolon in schema"),
    ],
)
def test_always_quote_join_identifier_quoting(nu, idents, blocked):
    result = nu.always_quote_join(*idents)
    stripped = re.sub(r'"[^"]*"', "", result)
    assert blocked not in stripped, f"Pattern {blocked!r} escaped quoting: {result!r}"
