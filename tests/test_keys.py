#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    inspect,
)
from sqlalchemy.testing.assertions import eq_


def test_composite_fk_reflects_key_order(engine_testaccount):
    metadata = MetaData()
    parent = Table(
        "test_keys_fk_parent_decl",
        metadata,
        Column("col_a", Integer),
        Column("col_b", Integer),
        Column("col_c", Integer),
        PrimaryKeyConstraint("col_a", "col_b", "col_c"),
    )
    child = Table(
        "test_keys_fk_child_decl",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("ref_a", Integer),
        Column("ref_b", Integer),
        Column("ref_c", Integer),
        ForeignKeyConstraint(
            ["ref_a", "ref_b", "ref_c"],
            [
                "test_keys_fk_parent_decl.col_a",
                "test_keys_fk_parent_decl.col_b",
                "test_keys_fk_parent_decl.col_c",
            ],
            name="fk_test_keys_decl",
        ),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)

        pk = inspector.get_pk_constraint("test_keys_fk_parent_decl")
        eq_(pk["constrained_columns"], ["col_a", "col_b", "col_c"])

        fks = inspector.get_foreign_keys("test_keys_fk_child_decl")
        eq_(len(fks), 1)
        eq_(fks[0]["name"], "fk_test_keys_decl")
        eq_(fks[0]["constrained_columns"], ["ref_a", "ref_b", "ref_c"])
        eq_(fks[0]["referred_columns"], ["col_a", "col_b", "col_c"])
        eq_(fks[0]["referred_table"], "test_keys_fk_parent_decl")
    finally:
        child.drop(engine_testaccount)
        parent.drop(engine_testaccount)


def test_composite_unique_reflects_key_order(engine_testaccount):
    metadata = MetaData()
    table = Table(
        "test_keys_uq_decl",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("col_x", Integer),
        Column("col_y", Integer),
        Column("col_z", Integer),
        UniqueConstraint("col_x", "col_y", "col_z", name="uq_test_keys_decl"),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        uqs = inspector.get_unique_constraints("test_keys_uq_decl")
        eq_(len(uqs), 1)
        eq_(uqs[0]["name"], "uq_test_keys_decl")
        eq_(uqs[0]["column_names"], ["col_x", "col_y", "col_z"])
    finally:
        table.drop(engine_testaccount)


def test_composite_pk_key_order_differs_from_table_column_order(engine_testaccount):
    metadata = MetaData()
    table = Table(
        "test_keys_pk_order",
        metadata,
        Column("col_first", Integer),
        Column("col_mid", Integer),
        Column("col_last", Integer),
        PrimaryKeyConstraint(
            "col_last", "col_first", "col_mid", name="pk_test_keys_order"
        ),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        pk = inspector.get_pk_constraint("test_keys_pk_order")
        eq_(pk["constrained_columns"], ["col_last", "col_first", "col_mid"])
    finally:
        table.drop(engine_testaccount)


def test_composite_unique_key_order_differs_from_table_column_order(engine_testaccount):
    metadata = MetaData()
    table = Table(
        "test_keys_uq_order",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("col_x", Integer),
        Column("col_y", Integer),
        Column("col_z", Integer),
        UniqueConstraint("col_z", "col_x", "col_y", name="uq_test_keys_order"),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        uqs = inspector.get_unique_constraints("test_keys_uq_order")
        eq_(len(uqs), 1)
        eq_(uqs[0]["name"], "uq_test_keys_order")
        eq_(uqs[0]["column_names"], ["col_z", "col_x", "col_y"])
    finally:
        table.drop(engine_testaccount)


def test_composite_fk_when_parent_pk_order_differs_from_columns(engine_testaccount):
    metadata = MetaData()
    parent = Table(
        "test_keys_fk_parent_mixed",
        metadata,
        Column("id", Integer),
        Column("attr", Integer),
        Column("name", Integer),
        PrimaryKeyConstraint("name", "id", "attr", name="pk_test_keys_mixed"),
    )
    child = Table(
        "test_keys_fk_child_mixed",
        metadata,
        Column("cid", Integer, primary_key=True),
        Column("pname", Integer),
        Column("pid", Integer),
        Column("pattr", Integer),
        ForeignKeyConstraint(
            ["pname", "pid", "pattr"],
            [
                "test_keys_fk_parent_mixed.name",
                "test_keys_fk_parent_mixed.id",
                "test_keys_fk_parent_mixed.attr",
            ],
            name="fk_test_keys_mixed",
        ),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)

        pk = inspector.get_pk_constraint("test_keys_fk_parent_mixed")
        eq_(pk["constrained_columns"], ["name", "id", "attr"])

        fks = inspector.get_foreign_keys("test_keys_fk_child_mixed")
        eq_(len(fks), 1)
        eq_(fks[0]["referred_columns"], ["name", "id", "attr"])
        eq_(fks[0]["constrained_columns"], ["pname", "pid", "pattr"])
    finally:
        child.drop(engine_testaccount)
        parent.drop(engine_testaccount)
