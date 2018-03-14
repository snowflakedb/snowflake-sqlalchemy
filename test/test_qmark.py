#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
#
import os

from parameters import (CONNECTION_PARAMETERS)

try:
    from parameters import (CONNECTION_PARAMETERS2)
except:
    CONNECTION_PARAMETERS2 = CONNECTION_PARAMETERS

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

import logging

for logger_name in ['snowflake.connector', 'botocore']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler('/tmp/python_connector.log')
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter(
        '%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)


def _get_engine_with_qmark(
        db_parameters, user=None, password=None, account=None):
    """
    Creates a connection with column metadata cache
    """
    if user is not None:
        db_parameters['user'] = user
    if password is not None:
        db_parameters['password'] = password
    if account is not None:
        db_parameters['account'] = account

    from sqlalchemy import create_engine
    from snowflake.sqlalchemy import URL

    engine = create_engine(URL(
        user=db_parameters['user'],
        password=db_parameters['password'],
        host=db_parameters['host'],
        port=db_parameters['port'],
        database=db_parameters['database'],
        schema=db_parameters['schema'],
        account=db_parameters['account'],
        protocol=db_parameters['protocol'],
    ))
    return engine


def test_qmark_bulk_insert(db_parameters):
    """
    Bulk insert using qmark paramstyle
    """
    import snowflake.connector
    snowflake.connector.paramstyle = u'qmark'

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
        for data in pd.read_sql_query("select * from src",
                                      engine, chunksize=16000):
            data.to_sql("dst", engine, if_exists='append',
                        index=False, index_label=None)

    finally:
        con.close()
        engine.dispose()
        snowflake.connector.paramstyle = u'pyformat'
