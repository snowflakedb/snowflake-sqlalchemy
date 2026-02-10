#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy.testing.provision import (
    create_db,
    drop_db,
    set_default_schema_on_connection,
)


@create_db.for_db("snowflake")
def _snowflake_create_db(cfg, eng, ident):
    """Create a schema for the xdist worker.

    For Snowflake, we create schemas instead of databases since:
    - Creating databases requires admin privileges
    - Schema-level isolation is sufficient for test isolation
    - The schema name becomes the 'ident' (e.g., test_schema_gw0)
    """
    with eng.begin() as conn:
        # Check if schema already exists
        result = conn.exec_driver_sql(
            f"SHOW SCHEMAS LIKE '{ident}' IN DATABASE {conn.connection.database}"
        )
        if not result.fetchone():
            conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {ident}")


@drop_db.for_db("snowflake")
def _snowflake_drop_db(cfg, eng, ident):
    """Drop the schema created for the xdist worker."""
    with eng.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {ident}")


# This is only for test purpose required by Requirement "default_schema_name_switch"
@set_default_schema_on_connection.for_db("snowflake")
def _snowflake_set_default_schema_on_connection(cfg, dbapi_connection, schema_name):
    cursor = dbapi_connection.cursor()
    cursor.execute(f"USE SCHEMA {dbapi_connection.database}.{schema_name};")
    cursor.close()
