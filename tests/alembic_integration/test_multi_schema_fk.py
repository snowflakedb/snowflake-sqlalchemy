#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""
Tests for Alembic autogenerate with multi-schema foreign keys.

This test validates that the fix for issue #610 resolves the problem where
Alembic autogenerate produces spurious CREATE TABLE and FK drop/recreate
operations for same-schema FKs in non-default schemas.
"""

import pytest
from sqlalchemy import Column, ForeignKey, Integer, MetaData, String, Table, text


def test_alembic_autogenerate_multi_schema_fk(multi_schema_setup):
    """
    Test that Alembic autogenerate correctly handles multi-schema FKs.

    This test:
    1. Creates tables with cross-schema and same-schema FKs in the database
    2. Defines ORM models matching those tables plus one extra column
    3. Runs compare_metadata() and verifies it only detects the column addition
    4. Ensures no spurious table creation or FK operations are generated
    """
    pytest.importorskip("alembic")
    from alembic.autogenerate import compare_metadata
    from alembic.migration import MigrationContext

    engine = multi_schema_setup["engine"]
    schema1 = multi_schema_setup["schema1"]
    schema2 = multi_schema_setup["schema2"]

    # Step 1: Create tables in the database
    with engine.connect() as conn:
        # Create table in schema1
        conn.execute(
            text(
                f"""
            CREATE TABLE {schema1}.users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            )
            """
            )
        )

        # Create table in schema2 with same-schema FK
        conn.execute(
            text(
                f"""
            CREATE TABLE {schema2}.products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            )
            """
            )
        )

        conn.execute(
            text(
                f"""
            CREATE TABLE {schema2}.orders (
                id INTEGER PRIMARY KEY,
                product_id INTEGER,
                user_id INTEGER,
                CONSTRAINT fk_same_schema FOREIGN KEY (product_id)
                    REFERENCES {schema2}.products(id),
                CONSTRAINT fk_cross_schema FOREIGN KEY (user_id)
                    REFERENCES {schema1}.users(id)
            )
            """
            )
        )
        conn.commit()

    # Step 2: Define metadata with models matching the database + one extra column
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
        Column("status", String(50)),  # New column not in database
        schema=schema2,
    )

    # Step 3: Run Alembic autogenerate comparison
    with engine.connect() as conn:
        migration_context = MigrationContext.configure(
            conn,
            opts={
                "compare_type": True,
                "compare_server_default": True,
                "include_schemas": True,
                "include_object": lambda obj, name, type_, reflected, compare_to: True,
            },
        )

        diff = compare_metadata(migration_context, target_metadata)

    # Step 4: Analyze the diff
    add_column_ops = [op for op in diff if op[0] == "add_column"]
    add_table_ops = [op for op in diff if op[0] == "add_table"]
    remove_fk_ops = [op for op in diff if op[0] == "remove_fk"]
    add_fk_ops = [op for op in diff if op[0] == "add_fk"]

    # Assertions
    assert (
        len(add_column_ops) == 1
    ), f"Expected 1 add_column operation, got {len(add_column_ops)}"
    assert add_column_ops[0][3].name == "status", "Expected to add 'status' column"

    assert (
        len(add_table_ops) == 0
    ), f"Should not create any tables, but got {add_table_ops}"
    assert (
        len(remove_fk_ops) == 0
    ), f"Should not remove any FKs, but got {remove_fk_ops}"
    assert len(add_fk_ops) == 0, f"Should not add any FKs, but got {add_fk_ops}"


def test_alembic_reflection_same_schema_fk(multi_schema_setup):
    """
    Direct test of FK reflection for same-schema FKs in non-default schemas.

    This is a simpler test that directly validates the referred_schema value
    without going through Alembic's full autogenerate machinery.
    """
    from sqlalchemy import inspect

    engine = multi_schema_setup["engine"]
    schema2 = multi_schema_setup["schema2"]

    # Create tables with same-schema FK
    with engine.connect() as conn:
        conn.execute(
            text(
                f"""
            CREATE TABLE {schema2}.categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100)
            )
            """
            )
        )

        conn.execute(
            text(
                f"""
            CREATE TABLE {schema2}.items (
                id INTEGER PRIMARY KEY,
                category_id INTEGER,
                CONSTRAINT fk_same_schema_test FOREIGN KEY (category_id)
                    REFERENCES {schema2}.categories(id)
            )
            """
            )
        )
        conn.commit()

    # Test FK reflection
    inspector = inspect(engine)
    fks = inspector.get_foreign_keys("items", schema=schema2)

    assert len(fks) == 1
    fk = fks[0]
    assert fk["name"] == "fk_same_schema_test"
    assert fk["referred_table"] == "categories"
    assert (
        fk["referred_schema"] is None
    ), f"Same-schema FK should have referred_schema=None, got {fk['referred_schema']}"
