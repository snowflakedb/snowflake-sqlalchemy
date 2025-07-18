#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

﻿#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os  # Used for placeholder credentials
from decimal import Decimal

from sqlalchemy import Float, MetaData, Table, cast, inspect, literal, select
from sqlalchemy.sql.ddl import CreateTable

import snowflake.connector
from snowflake.sqlalchemy import DECFLOAT

# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#


def test_select_decfloat_native_connector(db_parameters, snapshot):
    conn = None
    try:
        conn = snowflake.connector.connect(**db_parameters)

        # Create a cursor object.
        cur = conn.cursor()

        ret = cur.execute("SELECT 123456789.12345678::decfloat").fetchone()

        assert isinstance(
            ret[0], Decimal
        ), f"Return type is {type(ret[0])} {snowflake.connector.__version__}"
        assert ret[0] == snapshot

    except snowflake.connector.errors.ProgrammingError as e:
        print(f"An error occurred: {e}")
        print(
            "Please ensure your Snowflake credentials and account identifier are correct."
        )
    finally:
        # Close the cursor and connection.
        if conn:
            if "cur" in locals() and not cur.is_closed():
                cur.close()
            if not conn.is_closed():
                conn.close()
                print("Connection closed.")


def test_select_decfloat(engine_testaccount, db_parameters, sql_compiler, snapshot):

    # Equivalent to SQL: SELECT CAST('123.45' AS FLOAT)
    select_stmt = select(cast(literal("123.45"), DECFLOAT))

    compiled_sql = str(select_stmt.compile(compile_kwargs={"literal_binds": True}))

    assert compiled_sql == "SELECT CAST('123.45' AS DECFLOAT) AS anon_1"

    with engine_testaccount.connect() as connection:
        result = connection.execute(select_stmt)
        breakpoint()

        # Assert that the returned value is correct.
        # The exact Python type (float, Decimal) depends on your DBAPI driver.
        assert result == snapshot
        print(f"Successfully executed. Result: {result}, Type: {type(result)}")
