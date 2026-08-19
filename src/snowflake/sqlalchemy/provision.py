#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy.testing.provision import (
    create_db,
    drop_db,
    follower_url_from_main,
    set_default_schema_on_connection,
)


@follower_url_from_main.for_db("snowflake")
def _snowflake_follower_url_from_main(url, ident):
    """Build the per-worker (xdist) follower URL for Snowflake.

    sqlalchemy.testing isolates each xdist worker by replacing the URL's
    ``database`` with the follower ``ident``. Snowflake isolates per-worker with
    a *schema* rather than a database (see :func:`_snowflake_create_db`), and the
    Snowflake dialect encodes database/schema as ``"<db>/<schema>"`` in
    ``url.database``. So keep the real database and point the follower at the
    ``ident`` schema; otherwise the worker would connect to a database named
    ``ident`` that does not exist, yielding "no current database" errors.
    """
    database = (url.database or "").split("/", 1)[0]
    return url.set(database=f"{database}/{ident}")


@create_db.for_db("snowflake")
def _snowflake_create_db(cfg, eng, ident):
    """Create the per-worker (xdist) follower schema.

    For Snowflake we create schemas instead of databases since:
    - Creating databases requires admin privileges
    - Schema-level isolation is sufficient for test isolation
    The schema is created in ``eng``'s current database (the real test database),
    and :func:`_snowflake_follower_url_from_main` points the worker at it.
    """
    with eng.begin() as conn:
        conn.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {ident}")


@drop_db.for_db("snowflake")
def _snowflake_drop_db(cfg, eng, ident):
    """Drop the per-worker (xdist) follower schema."""
    with eng.begin() as conn:
        conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {ident}")


# This is only for test purpose required by Requirement "default_schema_name_switch"
@set_default_schema_on_connection.for_db("snowflake")
def _snowflake_set_default_schema_on_connection(cfg, dbapi_connection, schema_name):
    cursor = dbapi_connection.cursor()
    cursor.execute(f"USE SCHEMA {dbapi_connection.database}.{schema_name};")
    cursor.close()
