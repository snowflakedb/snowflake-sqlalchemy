#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""Unit tests for SNOW-1232488 — case-sensitive identifier support."""

from unittest import mock

import pytest
from sqlalchemy.engine.url import URL as SAUrl
from sqlalchemy.sql.elements import quoted_name

from snowflake.sqlalchemy import base
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


# ---------------------------------------------------------------------------
# Bug 1 — denormalize_column_name respects quoted_name.quote
# ---------------------------------------------------------------------------


class TestDenormalizeColumnName:
    """Bug 1: denormalize_column_name must quote when given quoted_name(x, True)."""

    def _get_compiler(self, dialect=None):
        """Return a SnowflakeDDLCompiler with a minimal dialect."""
        if dialect is None:
            dialect = base.dialect()
        from sqlalchemy import MetaData, Table, Column, Integer

        t = Table("t", MetaData(), Column("id", Integer))
        # Create a DDL compiler; the simplest way is to use the dialect's
        # ddl_compiler class.
        from sqlalchemy.sql.compiler import DDLCompiler

        # We just need the preparer attribute—grab it from the dialect.
        compiler_cls = dialect.ddl_compiler
        # Create a minimal object: DDLCompiler requires (dialect, statement, ...)
        # Using a real table's create statement as the "statement"
        from sqlalchemy.schema import CreateTable

        stmt = CreateTable(t)
        compiler = compiler_cls(dialect, stmt)
        return compiler

    def test_quoted_name_true_returns_quoted(self):
        """quoted_name('mycol', True) must return '"mycol"' (double-quoted)."""
        compiler = self._get_compiler()
        result = compiler.denormalize_column_name(quoted_name("mycol", True))
        # Should produce: "mycol" (the identifier surrounded by double quotes)
        assert result == '"mycol"', f"Expected '\"mycol\"', got {result!r}"

    def test_plain_lowercase_not_quoted(self):
        """Plain lowercase 'mycol' must remain unquoted (regression guard)."""
        compiler = self._get_compiler()
        result = compiler.denormalize_column_name("mycol")
        assert result == "mycol", f"Expected 'mycol', got {result!r}"

    def test_none_returns_none(self):
        """None input must return None."""
        compiler = self._get_compiler()
        assert compiler.denormalize_column_name(None) is None

    def test_mixed_case_returns_quoted(self):
        """Mixed-case name like 'myCol' requires quoting in Snowflake."""
        compiler = self._get_compiler()
        result = compiler.denormalize_column_name("myCol")
        assert result == '"myCol"', f"Expected '\"myCol\"', got {result!r}"

    def test_quoted_name_false_falls_through(self):
        """quoted_name('mycol', False) should follow normal branch (no forced quote)."""
        compiler = self._get_compiler()
        # quote=False means "let the dialect decide" — for simple lowercase it
        # should come back unquoted.
        result = compiler.denormalize_column_name(quoted_name("mycol", False))
        assert result == "mycol", f"Expected 'mycol', got {result!r}"


# ---------------------------------------------------------------------------
# Bug 2 — normalize_name reserved-word handling gated behind flag
# ---------------------------------------------------------------------------


class TestNormalizeNameReservedWord:
    """Bug 2: normalize_name('TABLE') with flag on vs off."""

    def _dialect(self, case_sensitive=False):
        return SnowflakeDialect(case_sensitive_identifiers=case_sensitive)

    def test_reserved_word_flag_off_returns_uppercase(self):
        """Default: normalize_name('TABLE') returns 'TABLE' unchanged (legacy)."""
        d = self._dialect(case_sensitive=False)
        result = d.normalize_name("TABLE")
        assert result == "TABLE", f"Expected 'TABLE', got {result!r}"
        # Must NOT be a quoted_name with quote=True
        assert not (
            isinstance(result, quoted_name) and result.quote
        ), "Should not be forced-quoted in default mode"

    def test_reserved_word_flag_on_returns_quoted_name(self):
        """With flag=True: normalize_name('TABLE') returns quoted_name('table', True)."""
        d = self._dialect(case_sensitive=True)
        result = d.normalize_name("TABLE")
        assert isinstance(result, quoted_name), f"Expected quoted_name, got {type(result)}"
        assert result.quote is True, "Expected quote=True"
        assert str(result) == "table", f"Expected 'table', got {str(result)!r}"

    def test_normal_uppercase_unaffected(self):
        """normalize_name('MYTABLE') still returns 'mytable' with both flag states."""
        for flag in (False, True):
            d = self._dialect(case_sensitive=flag)
            result = d.normalize_name("MYTABLE")
            assert result == "mytable", (
                f"normalize_name('MYTABLE') should be 'mytable' with flag={flag}, got {result!r}"
            )

    def test_lowercase_returns_quoted_name(self):
        """normalize_name('mytable') always returns quoted_name('mytable', True)."""
        for flag in (False, True):
            d = self._dialect(case_sensitive=flag)
            result = d.normalize_name("mytable")
            assert isinstance(result, quoted_name), (
                f"Expected quoted_name for 'mytable' with flag={flag}"
            )
            assert result.quote is True

    def test_none_and_empty(self):
        """None and '' are handled correctly with both flag states."""
        for flag in (False, True):
            d = self._dialect(case_sensitive=flag)
            assert d.normalize_name(None) is None
            assert d.normalize_name("") == ""

    def test_all_reserved_words_flag_off_unchanged(self):
        """All RESERVED_WORDS ALL-UPPERCASE identifiers return unchanged when flag=False."""
        from snowflake.sqlalchemy.base import RESERVED_WORDS

        d = self._dialect(case_sensitive=False)
        for word in list(RESERVED_WORDS)[:20]:  # spot-check first 20
            upper = word.upper()
            if upper == word:  # only truly all-uppercase reserved words
                result = d.normalize_name(upper)
                # With flag off, reserved-word UPPERCASE names fall through unchanged
                assert result == upper, (
                    f"flag=False: normalize_name({upper!r}) should be {upper!r}, got {result!r}"
                )

    def test_all_reserved_words_flag_on_returns_quoted(self):
        """With flag=True, ALL-UPPERCASE reserved words return quoted_name(lc, True)."""
        from snowflake.sqlalchemy.base import RESERVED_WORDS

        d = self._dialect(case_sensitive=True)
        for word in list(RESERVED_WORDS)[:20]:  # spot-check first 20
            upper = word.upper()
            if upper == word:
                result = d.normalize_name(upper)
                assert isinstance(result, quoted_name), (
                    f"flag=True: normalize_name({upper!r}) should be quoted_name, got {result!r}"
                )
                assert result.quote is True
                assert str(result) == upper.lower()


# ---------------------------------------------------------------------------
# Bug 3 — _has_object normalizes object_name before building SQL
# ---------------------------------------------------------------------------


class TestHasObjectNormalization:
    """Bug 3: _has_object should denormalize object_name before DESC SQL."""

    def _dialect_with_mock_connection(self):
        d = SnowflakeDialect()
        conn = mock.MagicMock()
        # Make the execute call return a result with one row so has_object=True
        result_mock = mock.MagicMock()
        result_mock.fetchone.return_value = ("some_row",)
        conn.execute.return_value = result_mock
        return d, conn

    def test_plain_lowercase_generates_quoted_uppercase(self):
        """_has_object('mytable') should generate DESC TABLE \"MYTABLE\" (quoted uppercase)."""
        d, conn = self._dialect_with_mock_connection()
        d._has_object(conn, "TABLE", "mytable")
        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        # denormalize_name('mytable') -> 'MYTABLE'
        # _denormalize_quote_join('MYTABLE') -> '"MYTABLE"'
        assert '"MYTABLE"' in sql_text, (
            f"Expected '\"MYTABLE\"' in DESC SQL, got: {sql_text!r}"
        )

    def test_quoted_name_generates_quoted_lowercase(self):
        """_has_object(quoted_name('mytable', True)) should generate DESC TABLE \"mytable\"."""
        d, conn = self._dialect_with_mock_connection()
        d._has_object(conn, "TABLE", quoted_name("mytable", True))
        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0])
        assert '"mytable"' in sql_text, (
            f"Expected '\"mytable\"' in DESC SQL, got: {sql_text!r}"
        )

    def test_programming_error_returns_false(self):
        """_has_object returns False when DESC raises ProgrammingError."""
        import sqlalchemy.exc as sa_exc
        from snowflake.connector import errors as sf_errors

        d = SnowflakeDialect()
        conn = mock.MagicMock()
        orig_exc = sf_errors.ProgrammingError()
        conn.execute.side_effect = sa_exc.DBAPIError(
            statement="DESC TABLE mytable",
            params={},
            orig=orig_exc,
            hide_parameters=False,
        )
        result = d._has_object(conn, "TABLE", "mytable")
        assert result is False


# ---------------------------------------------------------------------------
# Feature flag — case_sensitive_identifiers constructor and URL wiring
# ---------------------------------------------------------------------------


class TestCaseSensitiveIdentifiersFlag:
    """Feature flag wiring via constructor and URL."""

    def test_constructor_default_false(self):
        """Default SnowflakeDialect() has case_sensitive_identifiers=False."""
        d = SnowflakeDialect()
        assert d._case_sensitive_identifiers is False
        assert d.name_utils.case_sensitive_identifiers is False

    def test_constructor_true(self):
        """SnowflakeDialect(case_sensitive_identifiers=True) propagates to name_utils."""
        d = SnowflakeDialect(case_sensitive_identifiers=True)
        assert d._case_sensitive_identifiers is True
        assert d.name_utils.case_sensitive_identifiers is True

    def test_url_param_true(self):
        """URL ?case_sensitive_identifiers=True propagates to both dialect and name_utils."""
        d = SnowflakeDialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query={"case_sensitive_identifiers": "True"},
        )
        d.create_connect_args(url)
        assert d._case_sensitive_identifiers is True
        assert d.name_utils.case_sensitive_identifiers is True

    def test_url_param_false(self):
        """URL ?case_sensitive_identifiers=False keeps both at False."""
        d = SnowflakeDialect(case_sensitive_identifiers=True)  # start True
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query={"case_sensitive_identifiers": "False"},
        )
        d.create_connect_args(url)
        assert d._case_sensitive_identifiers is False
        assert d.name_utils.case_sensitive_identifiers is False

    def test_url_param_not_forwarded_to_connector(self):
        """case_sensitive_identifiers must be popped and not forwarded to connector."""
        d = SnowflakeDialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query={"case_sensitive_identifiers": "True"},
        )
        _, opts = d.create_connect_args(url)
        assert "case_sensitive_identifiers" not in opts, (
            "case_sensitive_identifiers must not be forwarded to connector opts"
        )


# ---------------------------------------------------------------------------
# URL encoding helper — create_snowflake_engine
# ---------------------------------------------------------------------------


class TestCreateSnowflakeEngineHelper:
    """Tests for the create_snowflake_engine URL-encoding helper."""

    def test_case_sensitive_schema_encodes_with_percent22(self):
        """case_sensitive_schema=True should encode schema as %22schema%22."""
        from snowflake.sqlalchemy.util import create_snowflake_engine

        # Use create_engine=False so we just get back the URL we'd use
        # We'll test the URL by creating a mock engine
        with mock.patch("snowflake.sqlalchemy.util._sa_create_engine") as mock_ce:
            mock_ce.return_value = mock.MagicMock()
            create_snowflake_engine(
                "snowflake://u:p@acct/mydb",
                schema="myschema",
                case_sensitive_schema=True,
            )
            call_url = mock_ce.call_args[0][0]
            assert "%22myschema%22" in call_url, (
                f"Expected %22myschema%22 in URL, got: {call_url!r}"
            )

    def test_case_insensitive_schema_no_encoding(self):
        """case_sensitive_schema=False (default) should not encode schema."""
        from snowflake.sqlalchemy.util import create_snowflake_engine

        with mock.patch("snowflake.sqlalchemy.util._sa_create_engine") as mock_ce:
            mock_ce.return_value = mock.MagicMock()
            create_snowflake_engine(
                "snowflake://u:p@acct/mydb",
                schema="myschema",
                case_sensitive_schema=False,
            )
            call_url = mock_ce.call_args[0][0]
            assert "%22" not in call_url, (
                f"Should not have percent-encoded quotes in URL, got: {call_url!r}"
            )
            assert "myschema" in call_url

    def test_no_schema(self):
        """Without schema, the base URL is passed through unchanged."""
        from snowflake.sqlalchemy.util import create_snowflake_engine

        base_url = "snowflake://u:p@acct/mydb"
        with mock.patch("snowflake.sqlalchemy.util._sa_create_engine") as mock_ce:
            mock_ce.return_value = mock.MagicMock()
            create_snowflake_engine(base_url)
            call_url = mock_ce.call_args[0][0]
            assert call_url == base_url


# ---------------------------------------------------------------------------
# Alembic helper
# ---------------------------------------------------------------------------


class TestAlembicRenderItemHelper:
    """Tests for alembic_util.render_item."""

    def test_importable(self):
        """render_item should be importable from snowflake.sqlalchemy.alembic_util."""
        from snowflake.sqlalchemy.alembic_util import render_item

        assert callable(render_item)

    def test_quoted_name_column_returns_string(self):
        """render_item for a quoted_name column must return a non-False string."""
        from snowflake.sqlalchemy.alembic_util import render_item
        from sqlalchemy import Column, Integer

        col = Column(quoted_name("mycol", True), Integer)

        autogen_ctx = mock.MagicMock()
        autogen_ctx.opts.__getitem__ = mock.MagicMock(
            side_effect=lambda k: mock.MagicMock()
        )

        result = render_item("column", col, autogen_ctx)
        assert result is not False, "render_item should return a string for quoted_name columns"
        assert isinstance(result, str), f"Expected str, got {type(result)}"
        assert "mycol" in result, f"Expected 'mycol' in result, got {result!r}"

    def test_plain_column_returns_false(self):
        """render_item for a plain (non-quoted) column returns False (delegate to default)."""
        from snowflake.sqlalchemy.alembic_util import render_item
        from sqlalchemy import Column, Integer

        col = Column("mycol", Integer)
        result = render_item("column", col, mock.MagicMock())
        assert result is False, f"Expected False for plain column, got {result!r}"

    def test_non_column_type_returns_false(self):
        """render_item for non-column types always returns False."""
        from snowflake.sqlalchemy.alembic_util import render_item

        result = render_item("table", mock.MagicMock(), mock.MagicMock())
        assert result is False

    def test_quoted_name_false_returns_false(self):
        """render_item for quoted_name('mycol', False) returns False (no forced quoting)."""
        from snowflake.sqlalchemy.alembic_util import render_item
        from sqlalchemy import Column, Integer

        col = Column(quoted_name("mycol", False), Integer)
        result = render_item("column", col, mock.MagicMock())
        assert result is False


# ---------------------------------------------------------------------------
# Structured type info manager cache key normalization (Gap 2)
# ---------------------------------------------------------------------------


class TestStructuredTypeCacheKeyNormalization:
    """Gap 2: _StructuredTypeInfoManager uses consistent normalized cache keys."""

    def _make_manager(self, mock_result=None):
        from snowflake.sqlalchemy.structured_type_info_manager import (
            _StructuredTypeInfoManager,
        )
        from snowflake.sqlalchemy.name_utils import _NameUtils
        from sqlalchemy.dialects.postgresql import dialect as pg_dialect

        d = SnowflakeDialect()
        name_utils = d.name_utils

        conn = mock.MagicMock()
        if mock_result is not None:
            conn.execute.return_value = mock_result
        else:
            conn.execute.return_value = iter([])

        manager = _StructuredTypeInfoManager(conn, name_utils, "PUBLIC")
        return manager, conn

    def test_uppercase_key_found_after_normalized_population(self):
        """After populating via get_column_info (normalized key), lookup must succeed."""
        # Simulates: Snowflake returns column name 'MYCOL' (uppercase = case-insensitive)
        # After normalize_name('MYCOL') -> 'mycol'
        # The cache key should be consistent so a second lookup doesn't miss.
        from snowflake.sqlalchemy.structured_type_info_manager import (
            _StructuredTypeInfoManager,
        )

        d = SnowflakeDialect()
        name_utils = d.name_utils

        # Simulate DESC TABLE returning one row: (col_name, type, ...)
        row = ("MYCOL", "VARCHAR(255)", None, "Y", None, "N", None, None, None, "")
        result_mock = mock.MagicMock()
        result_mock.__iter__ = mock.MagicMock(return_value=iter([row]))

        conn = mock.MagicMock()
        conn.execute.return_value = result_mock

        manager = _StructuredTypeInfoManager(conn, name_utils, "PUBLIC")

        # Populate the cache via get_column_info
        # schema='PUBLIC', table='MYTABLE' (both uppercase from Snowflake)
        # normalize_name('MYTABLE') -> 'mytable', normalize_name('MYCOL') -> 'mycol'
        # The cache key should be ('PUBLIC', 'MYTABLE') for the outer manager key
        # and inner dict key should be 'mycol'

        # Force population via _load_structured_type_info
        manager._load_structured_type_info("PUBLIC", "MYTABLE")

        # The key stored internally uses the names as passed; let's verify
        # the column lookup normalizes correctly.
        # After DESC, column_name 'MYCOL' is normalized to 'mycol' in _parse_desc_result.
        assert ("PUBLIC", "MYTABLE") in manager.full_columns_descriptions, (
            "Cache should be populated for ('PUBLIC', 'MYTABLE')"
        )
        inner = manager.full_columns_descriptions[("PUBLIC", "MYTABLE")]
        # The column name is normalized
        assert "mycol" in inner, f"Expected 'mycol' in inner dict, got keys: {list(inner.keys())}"
