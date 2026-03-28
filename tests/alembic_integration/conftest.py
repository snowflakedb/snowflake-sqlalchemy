#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import uuid

import pytest
from sqlalchemy import text


@pytest.fixture(scope="function")
def multi_schema_setup(engine_testaccount, db_parameters):
    """
    Creates two test schemas for multi-schema FK testing.
    Cleans up automatically after test.
    """
    schema1_name = f"test_alembic_schema1_{str(uuid.uuid4()).replace('-', '_')}"
    schema2_name = f"test_alembic_schema2_{str(uuid.uuid4()).replace('-', '_')}"
    default_schema = db_parameters.get("schema")

    with engine_testaccount.connect() as conn:
        # Create test schemas
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema1_name}"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema2_name}"))
        conn.commit()

    yield {
        "schema1": schema1_name,
        "schema2": schema2_name,
        "default_schema": default_schema,
        "engine": engine_testaccount,
    }

    # Cleanup
    with engine_testaccount.connect() as conn:
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema1_name} CASCADE"))
        conn.execute(text(f"DROP SCHEMA IF EXISTS {schema2_name} CASCADE"))
        conn.commit()
