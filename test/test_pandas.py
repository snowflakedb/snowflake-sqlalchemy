#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#
import random
import string

import pytest
import numpy as np
import pandas as pd
import sqlalchemy
import snowflake.sqlalchemy
from sqlalchemy import (Table, Column, Integer, String, MetaData,
                        Sequence, ForeignKey)


def _create_users_addresses_tables(engine_testaccount, metadata):
    users = Table('users', metadata,
                  Column('id', Integer, Sequence('user_id_seq'),
                         primary_key=True),
                  Column('name', String),
                  Column('fullname', String),
                  )

    addresses = Table('addresses', metadata,
                      Column('id', Integer, Sequence('address_id_seq'),
                             primary_key=True),
                      Column('user_id', None, ForeignKey('users.id')),
                      Column('email_address', String, nullable=False)
                      )
    metadata.create_all(engine_testaccount)
    return users, addresses


def _create_users_addresses_tables_without_sequence(engine_testaccount,
                                                    metadata):
    users = Table('users', metadata,
                  Column('id', Integer, primary_key=True),
                  Column('name', String),
                  Column('fullname', String),
                  )

    addresses = Table('addresses', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('user_id', None, ForeignKey('users.id')),
                      Column('email_address', String, nullable=False)
                      )
    metadata.create_all(engine_testaccount)
    return users, addresses


def test_a_simple_read_sql(engine_testaccount):
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables(
        engine_testaccount, metadata)

    try:
        # inserts data with an implicitly generated id
        ins = users.insert().values(name='jack', fullname='Jack Jones')
        results = engine_testaccount.execute(ins)
        assert results.inserted_primary_key == [1], 'sequence value'
        results.close()

        # inserts data with the given id
        conn = engine_testaccount.connect()
        ins = users.insert()
        conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams')

        df = pd.read_sql("SELECT * FROM users WHERE name =%(name)s",
                         params={'name': 'jack'}, con=engine_testaccount)

        assert len(df.values) == 1
        assert df.values[0][0] == 1
        assert df.values[0][1] == 'jack'
        assert hasattr(df, 'id')
        assert hasattr(df, 'name')
        assert hasattr(df, 'fullname')
    finally:
        # drop tables
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def get_engine_with_numpy(db_parameters, user=None, password=None,
                          account=None):
    """
    Creates a connection using the parameters defined in JDBC connect string
    """
    from sqlalchemy import create_engine
    from snowflake.sqlalchemy import URL

    if user is not None:
        db_parameters['user'] = user
    if password is not None:
        db_parameters['password'] = password
    if account is not None:
        db_parameters['account'] = account

    from sqlalchemy.pool import NullPool
    engine = create_engine(URL(
        user=db_parameters['user'],
        password=db_parameters['password'],
        host=db_parameters['host'],
        port=db_parameters['port'],
        database=db_parameters['database'],
        schema=db_parameters['schema'],
        account=db_parameters['account'],
        protocol=db_parameters['protocol'],
        numpy=True,
    ), poolclass=NullPool)

    return engine


def test_numpy_datatypes(db_parameters):
    engine = get_engine_with_numpy(db_parameters)
    try:
        specific_date = np.datetime64('2016-03-04T12:03:05.123456789')
        engine.execute(
            "CREATE OR REPLACE TABLE {name}("
            "c1 timestamp_ntz)".format(name=db_parameters['name']))
        engine.execute(
            "INSERT INTO {name}(c1) values(%s)".format(
                name=db_parameters['name']), (specific_date,)
        )
        df = pd.read_sql_query(
            "SELECT * FROM {name}".format(
                name=db_parameters['name']
            ), engine
        )
        assert df.c1.values[0] == specific_date
    finally:
        engine.execute(
            "DROP TABLE IF EXISTS {name}".format(name=db_parameters['name'])
        )
        engine.dispose()


def test_to_sql(db_parameters):
    engine = get_engine_with_numpy(db_parameters)
    total_rows = 10000
    engine.execute("""
create or replace table src(c1 float)
 as select random(123) from table(generator(timelimit=>1))
 limit {0}
""".format(total_rows))
    engine.execute("""
create or replace table dst(c1 float)
""")
    tbl = pd.read_sql_query(
        'select * from src', engine)

    tbl.to_sql('dst', engine, if_exists='append', chunksize=1000, index=False)
    df = pd.read_sql_query(
        'select count(*) as cnt from dst', engine
    )
    assert df.cnt.values[0] == total_rows

def test_no_indexes(engine_testaccount, db_parameters):
    conn = engine_testaccount.connect()
    data = pd.DataFrame([('1.0.0',), ('1.0.1',)])
    with pytest.raises(NotImplementedError) as exc:
        data.to_sql('versions', schema=db_parameters['schema'], index=True, index_label='col1', con=conn, if_exists='replace')
    assert str(exc.value) == "Snowflake does not support indexes"

def test_timezone(db_parameters):

    test_table_name = ''.join([random.choice(string.ascii_letters) for _ in range(5)])

    sa_engine = sqlalchemy.create_engine(snowflake.sqlalchemy.URL(
        account=db_parameters['account'],
        password=db_parameters['password'],
        database=db_parameters['database'],
        port=db_parameters['port'],
        user=db_parameters['user'],
        host=db_parameters['host'],
        protocol=db_parameters['protocol'],
        schema=db_parameters['schema'],
        numpy=True,
    ))

    sa_engine2 = sqlalchemy.create_engine(snowflake.sqlalchemy.URL(
    account=db_parameters['account'],
    password=db_parameters['password'],
    database=db_parameters['database'],
    port=db_parameters['port'],
    user=db_parameters['user'],
    host=db_parameters['host'],
    protocol=db_parameters['protocol'],
    schema=db_parameters['schema'],
    timezone='America/Los_Angeles',
    numpy='')).raw_connection()

    sa_engine.execute("""
    CREATE OR REPLACE TABLE {table}(
        tz_col timestamp_tz,
        ntz_col timestamp_ntz,
        decimal_col decimal(10,1),
        float_col float
    );""".format(table=test_table_name))

    try:
        sa_engine.execute("""
        INSERT INTO {table}
            SELECT
                current_timestamp(),
                current_timestamp()::timestamp_ntz,
                to_decimal(.1, 10, 1),
                .10;""".format(table=test_table_name))

        qry = """
        SELECT
            tz_col,
            ntz_col,
            CONVERT_TIMEZONE('America/Los_Angeles', tz_col) AS tz_col_converted,
            CONVERT_TIMEZONE('America/Los_Angeles', ntz_col) AS ntz_col_converted,
            decimal_col,
            float_col
        FROM {table};""".format(table=test_table_name)

        result = pd.read_sql_query(qry, sa_engine)
        result2 = pd.read_sql_query(qry, sa_engine2)
        # Check sqlalchemy engine result
        assert(list(result.tz_col.dtype._metadata) == ['unit', 'tz'])
        assert(result.ntz_col.dtype.type == np.datetime64)
        assert(list(result.tz_col_converted.dtype._metadata) == ['unit', 'tz'])
        assert(list(result.ntz_col_converted.dtype._metadata) == ['unit', 'tz'])
        assert(result.decimal_col.dtype.type == np.float64)
        assert(result.float_col.dtype == np.float64)
        # Check sqlalchemy raw connection result
        assert(list(result2.TZ_COL.dtype._metadata) == ['unit', 'tz'])
        assert(result2.NTZ_COL.dtype.type == np.datetime64)
        assert(list(result2.TZ_COL_CONVERTED.dtype._metadata) == ['unit', 'tz'])
        assert(list(result2.NTZ_COL_CONVERTED.dtype._metadata) == ['unit', 'tz'])
        assert(result2.DECIMAL_COL.dtype.type == np.float64)
        assert(result2.FLOAT_COL.dtype == np.float64)
    finally:
        sa_engine.execute('DROP TABLE {table};'.format(table=test_table_name))
