#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy.testing.provision import set_default_schema_on_connection


@set_default_schema_on_connection.for_db("snowflake")
def _snowflake_set_default_schema_on_connection(cfg, dbapi_connection, schema_name):
    cursor = dbapi_connection.cursor()
    cursor.execute(f"USE SCHEMA {dbapi_connection.database}.{schema_name};")
    cursor.close()
