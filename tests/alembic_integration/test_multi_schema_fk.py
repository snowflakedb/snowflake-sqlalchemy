#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Tests for Alembic autogenerate with multi-schema foreign keys (#610)."""

import uuid

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table, text


def _diff_op_name(diff_entry):
    if isinstance(diff_entry, tuple) and diff_entry:
        return diff_entry[0]
    return diff_entry.__class__.__name__


def _find_added_column(diff, column_name):
    for entry in diff:
        if isinstance(entry, tuple) and entry and entry[0] == "add_column":
            column = entry[3]
            if getattr(column, "name", None) == column_name:
                return entry
    return None


def test_alembic_autogenerate_multi_schema_fk(engine_testaccount):
    """Autogenerate only detects real changes, not spurious FK/table ops.

    DDL setup and compare_metadata run on the same connection so that
    SHOW SCHEMAS sees the newly-created schemas immediately (avoids
    metadata-visibility delays on some Snowflake cloud providers).
    """
    engine = engine_testaccount
    schema1 = f"test_alembic_schema1_{uuid.uuid4().hex}"
    schema2 = f"test_alembic_schema2_{uuid.uuid4().hex}"

    diff = None
    try:
        with engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema1}"))
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema2}"))
            conn.execute(
                text(
                    f"""
                CREATE TABLE {schema1}.users (
                    id INTEGER PRIMARY KEY, name VARCHAR(100))
            """
                )
            )
            conn.execute(
                text(
                    f"""
                CREATE TABLE {schema2}.products (
                    id INTEGER PRIMARY KEY, name VARCHAR(100))
            """
                )
            )
            conn.execute(
                text(
                    f"""
                CREATE TABLE {schema2}.orders (
                    id INTEGER PRIMARY KEY,
                    product_id INTEGER, user_id INTEGER,
                    CONSTRAINT fk_same_schema
                        FOREIGN KEY (product_id) REFERENCES {schema2}.products(id),
                    CONSTRAINT fk_cross_schema
                        FOREIGN KEY (user_id) REFERENCES {schema1}.users(id))
            """
                )
            )
            conn.commit()

            # Prime Snowflake metadata cache for newly created schemas/tables
            # so that SHOW SCHEMAS and SHOW TABLES see them immediately.
            # AWS has higher metadata propagation latency than Azure/GCP:
            # SHOW SCHEMAS and SHOW TABLES each have their own server-side
            # metadata cache, so both must be primed separately.
            conn.execute(text("SHOW SCHEMAS"))
            conn.execute(text(f"SHOW TABLES IN SCHEMA {schema1}"))
            conn.execute(text(f"SHOW TABLES IN SCHEMA {schema2}"))

            # Target metadata matches DB but adds one extra column.
            # Built after commit so schemas/tables are visible to compare_metadata
            # running on the same connection.
            target_metadata = MetaData()
            Table(
                "users",
                target_metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                schema=schema1,
            )
            Table(
                "products",
                target_metadata,
                Column("id", Integer, primary_key=True),
                Column("name", String(100)),
                schema=schema2,
            )
            Table(
                "orders",
                target_metadata,
                Column("id", Integer, primary_key=True),
                Column(
                    "product_id",
                    Integer,
                    ForeignKey(f"{schema2}.products.id", name="fk_same_schema"),
                ),
                Column(
                    "user_id",
                    Integer,
                    ForeignKey(f"{schema1}.users.id", name="fk_cross_schema"),
                ),
                Column("status", String(50)),
                schema=schema2,
            )

            test_schemas = {schema1, schema2}

            context = MigrationContext.configure(
                conn,
                opts={
                    "compare_type": True,
                    "compare_server_default": True,
                    "include_schemas": True,
                    "include_object": lambda obj, name, type_, reflected, compare_to: True,
                    "include_name": lambda name, type_, parent_names: (
                        name in test_schemas if type_ == "schema" else True
                    ),
                },
            )
            diff = compare_metadata(context, target_metadata)

    finally:
        with engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema2} CASCADE"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema1} CASCADE"))
            conn.commit()

    add_column_op = _find_added_column(diff, "status")
    assert add_column_op is not None
    assert not [op for op in diff if _diff_op_name(op) == "add_table"]
    # Both FKs are fully qualified in user metadata (schema2.products.id and
    # schema1.users.id), and the dialect preserves the real schema for FK
    # targets in non-default schemas, so Alembic sees no FK churn.
    fk_ops = [op for op in diff if _diff_op_name(op) in ("remove_fk", "add_fk")]
    assert fk_ops == []
