import numpy as np
import pandas as pd
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
        specific_date = np.datetime64('2016-03-04T12:03:05.123456789Z')
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
