#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Tests for Alembic autogenerate with multi-schema foreign keys (#610)."""

import uuid

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table, text
from sqlalchemy.engine.reflection import Inspector


class _CatalogSchemaInspector(Inspector):
    """Inspector that discovers schemas via information_schema.schemata.

    Snowflake's ``SHOW SCHEMAS`` uses a service-level metadata cache that
    can take minutes to reflect newly created schemas on AWS.
    ``information_schema.schemata`` reads from the catalog directly and
    sees committed DDL changes immediately.

    SQLAlchemy's ``inspect(connection)`` uses ``dialect.inspector`` when
    present (see ``Inspector._construct``), so setting this class on the
    dialect before calling Alembic's ``compare_metadata`` is enough to
    redirect schema discovery without monkey-patching.
    """

    def get_schema_names(self, **kw):
        with self._operation_context() as conn:
            rows = conn.execute(
                text("SELECT schema_name FROM information_schema.schemata")
            )
            return [conn.dialect.normalize_name(row[0]) for row in rows]


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
    """Autogenerate only detects real changes, not spurious FK/table ops."""
    engine = engine_testaccount
    dialect = engine.dialect
    schema1 = f"test_alembic_schema1_{uuid.uuid4().hex}"
    schema2 = f"test_alembic_schema2_{uuid.uuid4().hex}"

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

    # Target metadata matches DB but adds one extra column.
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

    # Use a custom Inspector that queries information_schema.schemata
    # for schema discovery.  SQLAlchemy's inspect(connection) checks
    # dialect.inspector and uses it when present (Inspector._construct).
    original_inspector = getattr(dialect, "inspector", None)
    dialect.inspector = _CatalogSchemaInspector
    try:
        with engine.connect() as cmp_conn:
            ctx = MigrationContext.configure(
                cmp_conn,
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
            diff = compare_metadata(ctx, target_metadata)
    finally:
        if original_inspector is None:
            del dialect.inspector
        else:
            dialect.inspector = original_inspector
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
