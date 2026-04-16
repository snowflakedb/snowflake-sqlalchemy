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
from sqlalchemy.testing import combinations
from sqlalchemy.testing.assertions import eq_


@combinations(
    (
        "declared-order",
        "test_keys_fk_parent_mixed",
        ["id", "attr", "name"],
        ["name", "id", "attr"],
        "test_keys_fk_child_mixed",
        ["cid", "pname", "pid", "pattr"],
        ["pname", "pid", "pattr"],
        "pk_test_keys_mixed",
        "fk_test_keys_mixed",
    ),
    id_="iaaaaaaaa",
)
def test_composite_fk_reflects_key_order(
    engine_testaccount,
    parent_table_name,
    parent_columns,
    parent_pk_order,
    child_table_name,
    child_columns,
    child_fk_order,
    pk_name,
    fk_name,
):
    """FK + PK column order matches the constraint declaration."""
    metadata = MetaData()
    parent = Table(
        parent_table_name,
        metadata,
        *(Column(column_name, Integer) for column_name in parent_columns),
        PrimaryKeyConstraint(*parent_pk_order, name=pk_name),
    )
    Table(
        child_table_name,
        metadata,
        Column(child_columns[0], Integer, primary_key=True),
        *(Column(column_name, Integer) for column_name in child_columns[1:]),
        ForeignKeyConstraint(
            child_fk_order,
            [f"{parent_table_name}.{column_name}" for column_name in parent_pk_order],
            name=fk_name,
        ),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)

        pk = inspector.get_pk_constraint(parent.name)
        eq_(pk["constrained_columns"], parent_pk_order)

        fks = inspector.get_foreign_keys(child_table_name)
        eq_(len(fks), 1)
        eq_(fks[0]["name"], fk_name)
        eq_(fks[0]["referred_table"], parent.name)
        eq_(fks[0]["referred_columns"], parent_pk_order)
        eq_(fks[0]["constrained_columns"], child_fk_order)
    finally:
        metadata.drop_all(engine_testaccount)


@combinations(
    (
        "declared-order",
        "test_keys_uq_decl",
        ["id", "col_x", "col_y", "col_z"],
        ["col_x", "col_y", "col_z"],
        "uq_test_keys_decl",
    ),
    (
        "column-order-differs",
        "test_keys_uq_order",
        ["id", "col_x", "col_y", "col_z"],
        ["col_z", "col_x", "col_y"],
        "uq_test_keys_order",
    ),
    id_="iaaaaa",
)
def test_composite_unique_reflects_key_order(
    engine_testaccount, table_name, table_columns, unique_order, constraint_name
):
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        *(
            Column(column_name, Integer, primary_key=(index == 0))
            for index, column_name in enumerate(table_columns)
        ),
        UniqueConstraint(*unique_order, name=constraint_name),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        uqs = inspector.get_unique_constraints(table_name)
        eq_(len(uqs), 1)
        eq_(uqs[0]["name"], constraint_name)
        eq_(uqs[0]["column_names"], unique_order)
    finally:
        metadata.drop_all(engine_testaccount)


@combinations(
    (
        "column-order-differs",
        "test_keys_pk_order",
        ["col_first", "col_mid", "col_last"],
        ["col_last", "col_first", "col_mid"],
        "pk_test_keys_order",
    ),
    id_="iaaaaa",
)
def test_composite_pk_reflects_key_order(
    engine_testaccount, table_name, table_columns, pk_order, constraint_name
):
    metadata = MetaData()
    Table(
        table_name,
        metadata,
        *(Column(column_name, Integer) for column_name in table_columns),
        PrimaryKeyConstraint(*pk_order, name=constraint_name),
    )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        pk = inspector.get_pk_constraint(table_name)
        eq_(pk["name"], constraint_name)
        eq_(pk["constrained_columns"], pk_order)
    finally:
        metadata.drop_all(engine_testaccount)
