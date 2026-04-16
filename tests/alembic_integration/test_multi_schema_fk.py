#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Tests for Alembic autogenerate with multi-schema foreign keys (#610)."""

import uuid

from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table, text


def test_alembic_autogenerate_multi_schema_fk(engine_testaccount):
    """Autogenerate only detects real changes, not spurious FK/table ops."""
    schema1 = f"test_alembic_schema1_{uuid.uuid4().hex}"
    schema2 = f"test_alembic_schema2_{uuid.uuid4().hex}"

    with engine_testaccount.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema1}"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema2}"))
        conn.commit()

    try:
        with engine_testaccount.connect() as conn:
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

        # Target metadata matches DB but adds one extra column
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

        with engine_testaccount.connect() as conn:
            context = MigrationContext.configure(
                conn,
                opts={
                    "compare_type": True,
                    "compare_server_default": True,
                    "include_schemas": True,
                    "include_object": lambda obj, name, type_, reflected, compare_to: True,
                },
            )
            diff = compare_metadata(context, target_metadata)

        add_column_ops = [op for op in diff if op[0] == "add_column"]
        assert len(add_column_ops) == 1
        assert add_column_ops[0][3].name == "status"
        assert not [op for op in diff if op[0] == "add_table"]
        assert not [op for op in diff if op[0] in ("remove_fk", "add_fk")]

    finally:
        with engine_testaccount.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema2} CASCADE"))
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema1} CASCADE"))
            conn.commit()


def test_alembic_autogenerate_fk_to_default_schema(engine_testaccount, db_parameters):
    """Autogenerate produces no spurious ops when metadata defines FK to default schema.

    Covers the case where a non-default schema table references the default schema.
    Reflection returns referred_schema=None for such FKs; the metadata FK must also
    omit the schema qualifier to avoid a spurious remove_fk/add_fk diff.
    """
    default_schema = db_parameters.get("schema")
    schema2 = f"test_alembic_fk_default_{uuid.uuid4().hex}"

    with engine_testaccount.connect() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema2}"))
        conn.commit()

    try:
        with engine_testaccount.connect() as conn:
            conn.execute(
                text(
                    f"""
                CREATE TABLE {default_schema}.categories (
                    id INTEGER PRIMARY KEY, name VARCHAR(100))
            """
                )
            )
            conn.execute(
                text(
                    f"""
                CREATE TABLE {schema2}.items (
                    id INTEGER PRIMARY KEY, category_id INTEGER,
                    CONSTRAINT fk_to_default
                        FOREIGN KEY (category_id) REFERENCES {default_schema}.categories(id))
            """
                )
            )
            conn.commit()

        # FK defined without schema qualifier — matches what reflection returns
        # (referred_schema=None for FKs targeting the default schema).
        target_metadata = MetaData()
        Table(
            "categories",
            target_metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(100)),
            schema=default_schema,
        )
        Table(
            "items",
            target_metadata,
            Column("id", Integer, primary_key=True),
            Column(
                "category_id",
                Integer,
                ForeignKey("categories.id", name="fk_to_default"),
            ),
            schema=schema2,
        )

        with engine_testaccount.connect() as conn:
            context = MigrationContext.configure(
                conn,
                opts={
                    "compare_type": True,
                    "compare_server_default": True,
                    "include_schemas": True,
                    "include_object": lambda obj, name, type_, reflected, compare_to: True,
                },
            )
            diff = compare_metadata(context, target_metadata)

        assert not [op for op in diff if op[0] == "add_table"]
        assert not [op for op in diff if op[0] in ("remove_fk", "add_fk")]

    finally:
        with engine_testaccount.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema2} CASCADE"))
            conn.execute(
                text(f"DROP TABLE IF EXISTS {default_schema}.categories CASCADE")
            )
            conn.commit()
