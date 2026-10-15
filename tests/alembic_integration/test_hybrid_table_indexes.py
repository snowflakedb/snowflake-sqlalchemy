#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Tests for the built-in SnowflakeImpl's hybrid table index handling."""

import uuid

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, inspect

from snowflake.sqlalchemy import HybridTable


def _diff_op_names(diff):
    # compare_metadata returns tuples, plus lists of tuples for column changes.
    for entry in diff:
        if isinstance(entry, list):
            yield from _diff_op_names(entry)
        elif isinstance(entry, tuple) and entry:
            yield entry[0]
        else:
            yield entry.__class__.__name__


@pytest.mark.aws
def test_autogenerate_ignores_fk_backing_indexes(engine_testaccount):
    """Autogenerate must not drop the index Snowflake builds for each FK."""
    engine = engine_testaccount
    suffix = uuid.uuid4().hex[:8]
    parent_name = f"test_fk_idx_parent_{suffix}"
    child_name = f"test_fk_idx_child_{suffix}"

    metadata = MetaData()
    HybridTable(
        parent_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(100)),
    )
    HybridTable(
        child_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "parent_id",
            Integer,
            ForeignKey(f"{parent_name}.id", name=f"fk_{child_name}_parent_id"),
        ),
    )
    metadata.create_all(engine)

    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn, opts={"compare_type": True})
            diff = compare_metadata(ctx, metadata)
    finally:
        metadata.drop_all(engine)

    op_names = list(_diff_op_names(diff))
    assert "remove_index" not in op_names
    assert "modify_type" not in op_names


@pytest.mark.aws
def test_consecutive_create_index_awaits_builds(engine_testaccount):
    """Back-to-back create_index calls on one hybrid table must not fail with
    391480 ("Another index is being built")."""
    engine = engine_testaccount
    table_name = f"test_consecutive_idx_{uuid.uuid4().hex[:8]}"

    metadata = MetaData()
    HybridTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("a", Integer),
        Column("b", Integer),
    )
    metadata.create_all(engine)

    try:
        with engine.begin() as conn:
            op = Operations(MigrationContext.configure(conn))
            op.create_index(f"ix_{table_name}_a", table_name, ["a"])
            op.create_index(f"ix_{table_name}_b", table_name, ["b"])

        index_names = {ix["name"] for ix in inspect(engine).get_indexes(table_name)}
    finally:
        metadata.drop_all(engine)

    assert {f"ix_{table_name}_a", f"ix_{table_name}_b"} <= index_names
