#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import re

import pytest

from snowflake.sqlalchemy.util import requires_quotes, split_identifier_parts

# ---------------------------------------------------------------------------
# split_identifier_parts — the dotted-identifier scanner
# ---------------------------------------------------------------------------


class TestSplitIdentifierParts:
    """split_identifier_parts returns ``(value, was_quoted)`` pairs and rejects
    malformed identifiers (text adjacent to a quoted segment without a
    separating dot, or an unterminated quote)."""

    @pytest.mark.parametrize(
        "text, expected",
        [
            pytest.param("", [], id="empty"),
            pytest.param("myschema", [("myschema", False)], id="single_plain"),
            pytest.param(
                "mydb.myschema",
                [("mydb", False), ("myschema", False)],
                id="dotted_plain",
            ),
            pytest.param('"myschema"', [("myschema", True)], id="single_quoted"),
            pytest.param(
                '"mydb"."myschema"',
                [("mydb", True), ("myschema", True)],
                id="dotted_quoted",
            ),
            pytest.param(
                '"mydb".myschema',
                [("mydb", True), ("myschema", False)],
                id="mixed_quote_first",
            ),
            pytest.param(
                'mydb."myschema"',
                [("mydb", False), ("myschema", True)],
                id="mixed_quote_second",
            ),
            # A dot inside a quoted segment is part of the identifier, not a split.
            pytest.param(
                '"my.schema"', [("my.schema", True)], id="dot_inside_quotes_not_split"
            ),
            # "" inside a quoted identifier is an escaped literal double-quote.
            pytest.param(
                '"my""schema"', [('my"schema', True)], id="embedded_escaped_quote"
            ),
            # Leading / trailing bare dots produce no empty parts.
            pytest.param("a.", [("a", False)], id="trailing_dot"),
            pytest.param(".a", [("a", False)], id="leading_dot"),
        ],
    )
    def test_split(self, text, expected):
        assert split_identifier_parts(text) == expected

    @pytest.mark.parametrize(
        "text",
        [
            pytest.param('prefix"QUOTED"', id="unquoted_before_quote"),
            pytest.param('a"B"', id="short_prefix"),
            pytest.param('tenantA_"TENANT_B"', id="underscore_prefix"),
            pytest.param('"QUOTED"suffix', id="unquoted_after_quote"),
            pytest.param('"unterminated', id="unterminated_quote"),
        ],
    )
    def test_malformed_identifier_raises(self, text):
        """A quoted segment must be dot-separated from any other part; adjacent
        text or an unterminated quote is rejected rather than parsed into an
        unintended multi-part reference."""
        with pytest.raises(ValueError):
            split_identifier_parts(text)


# ---------------------------------------------------------------------------
# requires_quotes — the quoting predicate
# ---------------------------------------------------------------------------

# Minimal synthetic config that mirrors the shape the preparer supplies, so the
# predicates can be tested in complete isolation from the dialect.
SYNTH_CFG = {
    "reserved_words": frozenset({"select", "table", "from"}),
    "illegal_identifiers": frozenset(set("0123456789") | {"_"}),
    "illegal_initial_characters": frozenset(set("0123456789") | {"$"}),
    "legal_characters": re.compile(r"^[A-Z0-9_$]+$", re.I),
}


class TestStructuralOnly:
    """requires_quotes(include_case=False) flags only *structural* danger — not mere case."""

    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param("", False, id="empty_is_safe"),
            pytest.param("mytable", False, id="plain_lower_safe"),
            pytest.param("MYTABLE", False, id="plain_upper_safe_snowflake_folds"),
            pytest.param("MyTable", False, id="mixed_case_safe_structurally"),
            pytest.param("col_1$x", False, id="legal_chars_safe"),
            # reserved word
            pytest.param("select", True, id="reserved_lower"),
            pytest.param("SELECT", True, id="reserved_upper"),
            # single-char illegal identifier
            pytest.param("_", True, id="lone_underscore"),
            pytest.param("5", True, id="lone_digit"),
            # illegal initial character
            pytest.param("1col", True, id="leading_digit"),
            pytest.param("$col", True, id="leading_dollar"),
            # characters outside the legal set => structural quoting risk
            pytest.param("a b", True, id="space"),
            pytest.param('a"b', True, id="embedded_quote"),
            pytest.param("a.b", True, id="dot"),
            pytest.param("a; -- aaa", True, id="semicolon"),
            pytest.param("a)b", True, id="paren"),
        ],
    )
    def test_structural_only(self, value, expected):
        assert requires_quotes(value, include_case=False, **SYNTH_CFG) is expected


class TestRequiresQuotes:
    """requires_quotes = structural check OR case-only clause."""

    @pytest.mark.parametrize(
        "value, expected",
        [
            pytest.param("mytable", False, id="plain_lower_no_quote"),
            pytest.param("col_1$x", False, id="legal_lower_no_quote"),
            # The case-only clause is the difference from the structural check:
            pytest.param("MyTable", True, id="mixed_case_requires_quote"),
            pytest.param("MYTABLE", True, id="upper_requires_quote"),
            # Structural cases still require quoting.
            pytest.param("select", True, id="reserved"),
            pytest.param("a b", True, id="space"),
            pytest.param("1col", True, id="leading_digit"),
        ],
    )
    def test_requires_quotes(self, value, expected):
        assert requires_quotes(value, **SYNTH_CFG) is expected

    def test_case_clause_is_the_only_difference(self):
        """For an all-uppercase legal name, requires_quotes diverges with and
        without ``include_case`` purely on the case clause."""
        assert requires_quotes("MYTABLE", include_case=False, **SYNTH_CFG) is False
        assert requires_quotes("MYTABLE", **SYNTH_CFG) is True

    def test_empty_string_is_safe_not_indexerror(self):
        """Composed form must not raise on '' (the old inline _requires_quotes
        indexed value[0] with no empty guard)."""
        assert requires_quotes("", **SYNTH_CFG) is False


# ---------------------------------------------------------------------------
# The predicates accept the real preparer config unchanged
# ---------------------------------------------------------------------------


class TestPredicatesWithRealDialectConfig:
    """Smoke test: the same functions work with the live dialect's config,
    proving the preparer delegation supplies a compatible bundle."""

    @pytest.fixture
    def cfg(self):
        from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

        ip = SnowflakeDialect().identifier_preparer
        return {
            "reserved_words": ip.reserved_words,
            "illegal_identifiers": ip.illegal_identifiers,
            "illegal_initial_characters": ip.illegal_initial_characters,
            "legal_characters": ip.legal_characters,
        }

    @pytest.mark.parametrize(
        "value, unsafe, needs_quotes",
        [
            ("mytable", False, False),
            ("MYTABLE", False, True),
            ("MyTable", False, True),
            ("table", True, True),  # reserved word
            ("select", True, True),
            ("a b", True, True),
            ('a"b', True, True),
        ],
    )
    def test_real_config(self, cfg, value, unsafe, needs_quotes):
        assert requires_quotes(value, include_case=False, **cfg) is unsafe
        assert requires_quotes(value, **cfg) is needs_quotes
