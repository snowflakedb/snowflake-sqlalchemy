#!/usr/bin/env python
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#
import os

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def _get_engine_with_qmark(db_parameters, user=None, password=None, account=None):
    """
    Creates a connection with column metadata cache
    """
    if user is not None:
        db_parameters["user"] = user
    if password is not None:
        db_parameters["password"] = password
    if account is not None:
        db_parameters["account"] = account

    from sqlalchemy import create_engine

    from snowflake.sqlalchemy import URL

    engine = create_engine(
        URL(
            user=db_parameters["user"],
            password=db_parameters["password"],
            host=db_parameters["host"],
            port=db_parameters["port"],
            database=db_parameters["database"],
            schema=db_parameters["schema"],
            account=db_parameters["account"],
            protocol=db_parameters["protocol"],
        )
    )
    return engine


def test_qmark_bulk_insert(db_parameters):
    """
    Bulk insert using qmark paramstyle
    """
    import snowflake.connector

    snowflake.connector.paramstyle = "qmark"

    engine = _get_engine_with_qmark(db_parameters)
    con = engine.connect()
    import pandas as pd

    try:
        con.execute(
            """
            create or replace table src(c1 int, c2 string) as select seq8(),
            randstr(100, random()) from table(generator(rowcount=>100000))
            """
        )
        con.execute(
            """
            create or replace table dst like src
            """
        )
        for data in pd.read_sql_query("select * from src", engine, chunksize=16000):
            data.to_sql(
                "dst", engine, if_exists="append", index=False, index_label=None
            )

    finally:
        con.close()
        engine.dispose()
        snowflake.connector.paramstyle = "pyformat"
