#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.sqlalchemy import FQN


@pytest.mark.parametrize(
    "raw, expected",
    [
        # single (unqualified) names
        ("pol", (None, None, "pol")),
        ("_pol", (None, None, "_pol")),
        ("pol_1", (None, None, "pol_1")),
        ("p$x", (None, None, "p$x")),
        ("MyPolicy", (None, None, "MyPolicy")),
        # quoted single names (preserved verbatim, incl. specials/dots inside)
        ('"with space"', (None, None, '"with space"')),
        ('"with""quote"', (None, None, '"with""quote"')),
        ('"weird.name"', (None, None, '"weird.name"')),
        # schema.name
        ("sch.pol", (None, "sch", "pol")),
        ('sch."Name"', (None, "sch", '"Name"')),
        ('"a.b".c', (None, '"a.b"', "c")),
        # database.schema.name
        ("db.sch.pol", ("db", "sch", "pol")),
        ('db."Sch".pol', ("db", '"Sch"', "pol")),
        ('"DB"."Sch"."Pol"', ('"DB"', '"Sch"', '"Pol"')),
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
        ('db."Sch".pol', ["db", '"Sch"', "pol"], 'db."Sch".pol'),
    ],
)
def test_fqn_parts_and_identifier(raw, parts, identifier):
    fqn = FQN.from_string(raw)
    assert fqn.parts == parts
    assert fqn.identifier == identifier
    assert str(fqn) == identifier


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
