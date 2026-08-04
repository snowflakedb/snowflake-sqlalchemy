#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.sqlalchemy import FQN
from snowflake.sqlalchemy.util import split_identifier_parts


@pytest.mark.parametrize(
    "raw, expected",
    [
        # single (unqualified) names
        ("pol", (None, None, "pol")),
        ("_pol", (None, None, "_pol")),
        ("pol_1", (None, None, "pol_1")),
        ("p$x", (None, None, "p$x")),
        ("MyPolicy", (None, None, "MyPolicy")),
        # quoted single names: the *value* is unquoted (quotes/escapes stripped);
        # the quote is re-applied by ``identifier``, not stored in the value.
        ('"with space"', (None, None, "with space")),
        ('"with""quote"', (None, None, 'with"quote')),
        ('"weird.name"', (None, None, "weird.name")),
        # schema.name
        ("sch.pol", (None, "sch", "pol")),
        ('sch."Name"', (None, "sch", "Name")),
        ('"a.b".c', (None, "a.b", "c")),
        # database.schema.name
        ("db.sch.pol", ("db", "sch", "pol")),
        ('db."Sch".pol', ("db", "Sch", "pol")),
        ('"DB"."Sch"."Pol"', ("DB", "Sch", "Pol")),
    ],
)
def test_fqn_from_string_parses(raw, expected):
    fqn = FQN.from_string(raw)
    assert (fqn.database, fqn.schema, fqn.name) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",  # empty
        "My Policy",  # unquoted space
        "1policy",  # leading digit
        "a..b",  # empty middle part
        ".pol",  # leading dot
        "pol.",  # trailing dot
        "a.b.c.d",  # too many parts
        '"unterminated',  # unbalanced quote
    ],
)
def test_fqn_from_string_invalid(raw):
    with pytest.raises(ValueError):
        FQN.from_string(raw)


@pytest.mark.parametrize(
    "raw, parts, identifier",
    [
        ("pol", ["pol"], "pol"),
        ("sch.pol", ["sch", "pol"], "sch.pol"),
        ("db.sch.pol", ["db", "sch", "pol"], "db.sch.pol"),
        # parts hold the *unquoted* values; identifier re-quotes as needed.
        ('db."Sch".pol', ["db", "Sch", "pol"], 'db."Sch".pol'),
        ('"a""b".c', ['a"b', "c"], '"a""b".c'),
    ],
)
def test_fqn_parts_and_identifier(raw, parts, identifier):
    fqn = FQN.from_string(raw)
    assert fqn.parts == parts
    assert fqn.identifier == identifier
    assert str(fqn) == identifier


@pytest.mark.parametrize(
    "raw, expected_flags",
    [
        ("db.sch.pol", [False, False, False]),
        ('db."Sch".pol', [False, True, False]),
        ('"a.b".c', [True, False]),
        ('"with""quote"', [True]),
    ],
)
def test_fqn_parts_carry_quote_flags(raw, expected_flags):
    # Each part is a quoted_name whose ``quote`` flag mirrors the source token,
    # matching the preparer's part model so parts can be quoted without
    # double-quoting.
    fqn = FQN.from_string(raw)
    assert [bool(getattr(p, "quote", None)) for p in fqn.parts] == expected_flags


@pytest.mark.parametrize(
    "raw",
    ["db.sch.pol", 'db."Sch".pol', '"a.b".c', '"a""b".c', '"weird.name"'],
)
def test_fqn_tokenization_matches_preparer_scanner(raw):
    # FQN must tokenize identically to the shared scanner the preparer uses,
    # so the two never disagree on values or quoting.
    fqn = FQN.from_string(raw)
    scanned = split_identifier_parts(raw)
    assert [(str(p), bool(getattr(p, "quote", None))) for p in fqn.parts] == [
        (value, bool(was_quoted)) for value, was_quoted in scanned
    ]


def test_fqn_equality_and_hash():
    a = FQN.from_string("db.sch.pol")
    b = FQN.from_string("db.sch.pol")
    c = FQN.from_string("sch.pol")
    assert a == b
    assert a != c
    assert hash(a) == hash(b)
    assert a != "db.sch.pol"


def test_fqn_direct_construction():
    fqn = FQN(name="pol", schema="sch", database="db")
    assert fqn.parts == ["db", "sch", "pol"]
    assert fqn.identifier == "db.sch.pol"
