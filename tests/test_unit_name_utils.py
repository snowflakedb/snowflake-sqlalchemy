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


class TestQuoteComponent:
    def test_plain_lowercase_is_uppercased_and_quoted(self, nu):
        assert nu._quote_component(quoted_name("myschema", None)) == '"MYSCHEMA"'

    def test_plain_uppercase_is_quoted(self, nu):
        assert nu._quote_component(quoted_name("MYSCHEMA", None)) == '"MYSCHEMA"'

    def test_mixed_case_without_explicit_quote_is_quoted(self, nu):
        assert nu._quote_component(quoted_name("MySchema", None)) == '"MySchema"'

    def test_quote_true_preserves_lowercase_no_denormalization(self, nu):
        # quote=True signals the identifier was double-quoted at the source and
        # is case-sensitive.  denormalize_name must NOT uppercase it.
        assert nu._quote_component(quoted_name("myschema", True)) == '"myschema"'

    def test_quote_true_mixed_case_preserved(self, nu):
        assert nu._quote_component(quoted_name("MySchema", True)) == '"MySchema"'

    def test_internal_double_quote_is_escaped(self, nu):
        result = nu._quote_component(quoted_name('my"schema', None))
        assert result.startswith('"') and result.endswith('"')
        assert '""' in result  # SQL double-quote escaping


# ---------------------------------------------------------------------------
# always_quote_join — normal identifiers
# ---------------------------------------------------------------------------


class TestAlwaysQuoteJoinNormal:
    def test_plain_lowercase_schema_and_table(self, nu):
        assert nu.always_quote_join("myschema", "mytable") == '"MYSCHEMA"."MYTABLE"'

    def test_plain_uppercase_schema_and_table(self, nu):
        assert nu.always_quote_join("MYSCHEMA", "MYTABLE") == '"MYSCHEMA"."MYTABLE"'

    def test_mixed_case_schema_and_table(self, nu):
        result = nu.always_quote_join("MySchema", "MyTable")
        assert result == '"MySchema"."MyTable"'

    def test_none_ident_is_skipped(self, nu):
        assert (
            nu.always_quote_join(None, "myschema", "mytable") == '"MYSCHEMA"."MYTABLE"'
        )

    def test_single_ident(self, nu):
        assert nu.always_quote_join("myschema") == '"MYSCHEMA"'


# ---------------------------------------------------------------------------
# always_quote_join — database-qualified schemas (1.10.1 regression guard)
# ---------------------------------------------------------------------------


class TestAlwaysQuoteJoinDatabaseQualified:
    def test_unquoted_db_qualified_schema_is_split(self, nu):
        # Primary regression: MYDB.MYSCHEMA must become "MYDB"."MYSCHEMA",
        # not "MYDB.MYSCHEMA" (a single identifier containing a literal dot).
        result = nu.always_quote_join("MYDB.MYSCHEMA", "mytable")
        assert result == '"MYDB"."MYSCHEMA"."MYTABLE"'

    def test_lowercase_db_qualified_schema_is_split_and_uppercased(self, nu):
        result = nu.always_quote_join("mydb.myschema", "mytable")
        assert result == '"MYDB"."MYSCHEMA"."MYTABLE"'

    def test_pre_quoted_db_qualified_schema_not_double_escaped(self, nu):
        # If the schema string already contains SQL double-quotes around each
        # component, the parser must strip and re-quote correctly.
        result = nu.always_quote_join('"MYDB"."MYSCHEMA"', "mytable")
        assert result == '"MYDB"."MYSCHEMA"."MYTABLE"'
        assert '"""' not in result

    def test_literal_dot_inside_quoted_component_not_split(self, nu):
        # A quoted component containing a literal dot must not be re-split.
        result = nu.always_quote_join('"my.schema"', "mytable")
        assert result == '"my.schema"."MYTABLE"'


# ---------------------------------------------------------------------------
# always_quote_join — case-sensitivity signal (quote=True) preserved
# ---------------------------------------------------------------------------


class TestAlwaysQuoteJoinCaseSensitivity:
    def test_quote_true_lowercase_schema_case_preserved(self, nu):
        # Old _always_quote_join called str() before checking quote, so
        # quoted_name("myschema", True) was uppercased to "MYSCHEMA".
        # always_quote_join must preserve the case-sensitive lowercase form.
        schema = quoted_name("myschema", True)
        result = nu.always_quote_join(schema, "mytable")
        assert '"myschema"' in result
        assert '"MYSCHEMA"' not in result

    def test_quote_true_mixed_case_schema_case_preserved(self, nu):
        schema = quoted_name("MySchema", True)
        result = nu.always_quote_join(schema, "mytable")
        assert '"MySchema"' in result


# ---------------------------------------------------------------------------
# always_quote_join — SQL injection guard
# ---------------------------------------------------------------------------


class TestAlwaysQuoteJoinInjection:
    def test_semicolon_in_schema_is_quoted(self, nu):
        payload = "myschema; DROP TABLE users--"
        result = nu.always_quote_join(payload, "mytable")
        stripped = re.sub(r'"[^"]*"', "", result)
        assert ";" not in stripped, f"Semicolon escaped quoting: {result!r}"

    def test_semicolon_in_table_is_quoted(self, nu):
        payload = "mytable; SELECT 1--"
        result = nu.always_quote_join("myschema", payload)
        stripped = re.sub(r'"[^"]*"', "", result)
        assert ";" not in stripped, f"Semicolon escaped quoting: {result!r}"

    def test_dot_in_table_splits_into_components(self, nu):
        # A dot in table_name produces extra components, but all are quoted.
        result = nu.always_quote_join("myschema", "foo.bar")
        assert result == '"MYSCHEMA"."FOO"."BAR"'

    def test_drop_injection_in_schema_is_neutralised(self, nu):
        payload = "x'; DROP TABLE t--"
        result = nu.always_quote_join(payload, "t")
        stripped = re.sub(r'"[^"]*"', "", result)
        assert "DROP" not in stripped, f"DROP keyword escaped quoting: {result!r}"
