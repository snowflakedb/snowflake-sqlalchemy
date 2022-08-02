#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import os
import sys

import pytest
from sqlalchemy import text

from snowflake.sqlalchemy import URL

from .conftest import create_engine_with_future_flag as create_engine

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


def test_qmark_bulk_insert(db_parameters, run_v20_sqlalchemy):
    """
    Bulk insert using qmark paramstyle
    """
    if run_v20_sqlalchemy and sys.version_info < (3, 8):
        pytest.skip(
            "In Python 3.7, this test depends on pandas features of which the implementation is incompatible with sqlachemy 2.0, and pandas does not support Python 3.7 anymore."
        )

    import snowflake.connector

    snowflake.connector.paramstyle = "qmark"

    engine = _get_engine_with_qmark(db_parameters)
    import pandas as pd

    with engine.connect() as con:
        try:
            with con.begin():
                con.exec_driver_sql(
                    """
                    create or replace table src(c1 int, c2 string) as select seq8(),
                    randstr(100, random()) from table(generator(rowcount=>100000))
                    """
                )
                con.exec_driver_sql("create or replace table dst like src")

            with con.begin():
                for data in pd.read_sql_query(
                    text("select * from src"), con, chunksize=16000
                ):
                    data.to_sql(
                        "dst", con, if_exists="append", index=False, index_label=None
                    )

        finally:
            engine.dispose()
            snowflake.connector.paramstyle = "pyformat"
