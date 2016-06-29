#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2016 Snowflake Computing Inc. All right reserved.
#
from parameters import (CONNECTION_PARAMETERS)

import pytest
from snowflake.sqlalchemy import URL
from sqlalchemy import (Table, Column, Integer, String, MetaData, Sequence,
                        ForeignKey)
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.sql import and_, or_, not_
from sqlalchemy.sql import select

try:
    from parameters import (CONNECTION_PARAMETERS2)
except:
    CONNECTION_PARAMETERS2 = CONNECTION_PARAMETERS


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


def test_connect_args():
    """
    Tests connect string

    Snowflake connect string supports accout name as a replacement of
    host:port
    """
    from sqlalchemy import create_engine
    engine = create_engine(
        'snowflake://{user}:{password}@{account}/{database}/{schema}'.format(
            user=CONNECTION_PARAMETERS2['user'],
            password=CONNECTION_PARAMETERS2['password'],
            account=CONNECTION_PARAMETERS2['account'],
            database=CONNECTION_PARAMETERS2['database'],
            schema=CONNECTION_PARAMETERS2['schema'],
        )
    )
    try:
        results = engine.execute('select current_version()').fetchone()
        assert results is not None
    finally:
        engine.dispose()

    engine = create_engine(
        'snowflake://{user}:{password}@{account}/'.format(
            user=CONNECTION_PARAMETERS2['user'],
            password=CONNECTION_PARAMETERS2['password'],
            account=CONNECTION_PARAMETERS2['account'],
        )
    )
    try:
        results = engine.execute('select current_version()').fetchone()
        assert results is not None
    finally:
        engine.dispose()

    engine = create_engine(URL(
        user=CONNECTION_PARAMETERS2['user'],
        password=CONNECTION_PARAMETERS2['password'],
        account=CONNECTION_PARAMETERS2['account'],
    )
    )
    try:
        results = engine.execute('select current_version()').fetchone()
        assert results is not None
    finally:
        engine.dispose()


def test_simple_sql(engine_testaccount):
    """
    Simple SQL by SQLAlchemy
    """
    result = engine_testaccount.execute('show databases')
    rows = [row for row in result]
    assert len(rows) >= 0, 'show database results'


def test_create_drop_tables(engine_testaccount):
    """
    Creates and Drops tables
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount, metadata)

    try:
        # validate the tables exists
        results = engine_testaccount.execute('desc table users')
        assert len([row for row in results]) > 0, "users table doesn't exist"

        # validate the tables exists
        results = engine_testaccount.execute('desc table addresses')
        assert len([row for row in results]) > 0, \
            "addresses table doesn't exist"
    finally:
        # drop tables
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_insert_tables(engine_testaccount):
    """
    Inserts data into tables
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables(engine_testaccount,
                                                      metadata)

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

        # verify the results
        s = select([users])
        results = conn.execute(s)
        assert len([row for row in results]) == 2, \
            'number of rows from users table'
        results.close()

        # fetchone
        s = select([users]).order_by('id')
        results = conn.execute(s)
        row = results.fetchone()
        results.close()
        assert row[2] == 'Jack Jones', 'user name'
        assert row['fullname'] == 'Jack Jones', "user name by dict"
        assert row[users.c.fullname] == 'Jack Jones', \
            'user name by Column object'

        conn.execute(addresses.insert(), [
            {'user_id': 1, 'email_address': 'jack@yahoo.com'},
            {'user_id': 1, 'email_address': 'jack@msn.com'},
            {'user_id': 2, 'email_address': 'www@www.org'},
            {'user_id': 2, 'email_address': 'wendy@aol.com'},
        ])

        # more records
        s = select([addresses])
        results = conn.execute(s)
        assert len([row for row in results]) == 4, \
            'number of rows from addresses table'
        results.close()

        # select specified column names
        s = select([users.c.name, users.c.fullname]).order_by('name')
        results = conn.execute(s)
        results.fetchone()
        row = results.fetchone()
        assert row['name'] == 'wendy', 'name'

        # join
        s = select([users, addresses]).where(users.c.id == addresses.c.user_id)
        results = conn.execute(s)
        results.fetchone()
        results.fetchone()
        results.fetchone()
        row = results.fetchone()
        assert row['email_address'] == 'wendy@aol.com', 'email address'

        # Operator
        assert str(users.c.id == addresses.c.user_id) == \
               'users.id = addresses.user_id', 'equal operator'
        assert str(users.c.id == 7) == 'users.id = :id_1', \
            'equal to a static number'
        assert str(users.c.name == None) == 'users.name IS NULL', \
            'equal to None'
        assert str(users.c.id + addresses.c.id) == 'users.id + addresses.id', \
            'number + number'
        assert str(users.c.name + users.c.fullname) == \
               'users.name || users.fullname', 'str + str'

        # Conjunctions
        # example 1
        obj = and_(
            users.c.name.like('j%'),
            users.c.id == addresses.c.user_id,
            or_(
                addresses.c.email_address == 'wendy@aol.com',
                addresses.c.email_address == 'jack@yahoo.com'
            ),
            not_(users.c.id > 5)
        )
        expected_sql = """users.name LIKE :name_1
 AND users.id = addresses.user_id
 AND (addresses.email_address = :email_address_1
 OR addresses.email_address = :email_address_2)
 AND users.id <= :id_1"""
        assert str(obj) == ''.join(expected_sql.split('\n')), \
            "complex condition"

        # example 2
        obj = users.c.name.like('j%') & (users.c.id == addresses.c.user_id) & \
              (
                  (addresses.c.email_address == 'wendy@aol.com') | \
                  (addresses.c.email_address == 'jack@yahoo.com')
              ) \
              & ~(users.c.id > 5)
        assert str(obj) == ''.join(expected_sql.split('\n')), \
            "complex condition using python operators"

        # example 3
        s = select([(users.c.fullname +
                     ", " + addresses.c.email_address).
                   label('title')]). \
            where(
            and_(
                users.c.id == addresses.c.user_id,
                users.c.name.between('m', 'z'),
                or_(
                    addresses.c.email_address.like('%@aol.com'),
                    addresses.c.email_address.like('%@msn.com')
                )
            )

        )
        results = engine_testaccount.execute(s).fetchall()
        assert results[0][0] == 'Wendy Williams, wendy@aol.com'

        # Aliases
        a1 = addresses.alias()
        a2 = addresses.alias()
        s = select([users]).where(and_(
            users.c.id == a1.c.user_id,
            users.c.id == a2.c.user_id,
            a1.c.email_address == 'jack@msn.com',
            a2.c.email_address == 'jack@yahoo.com'))
        results = engine_testaccount.execute(s).fetchone()
        assert results == (1, 'jack', 'Jack Jones')

        # Joins
        assert str(users.join(addresses)) == 'users JOIN addresses ON ' \
                                             'users.id = addresses.user_id'
        assert str(users.join(addresses,
                              addresses.c.email_address.like(
                                  users.c.name + '%'))) == \
               'users JOIN addresses ' \
               'ON addresses.email_address LIKE (users.name || :name_1)'

        s = select([users.c.fullname]).select_from(
            users.join(addresses,
                       addresses.c.email_address.like(users.c.name + '%')))
        results = engine_testaccount.execute(s).fetchall()
        assert results[1] == ('Jack Jones',)

        s = select([users.c.fullname]).select_from(users.outerjoin(
            addresses)).order_by(users.c.fullname)
        results = engine_testaccount.execute(s).fetchall()
        assert results[-1] == ('Wendy Williams',)
    finally:
        # drop tables
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


@pytest.mark.skip("""
Reflection is not implemented yet.
""")
def test_reflextion(engine_testaccount):
    """
    Tests Reflection
    """
    engine_testaccount.execute("""
CREATE OR REPLACE TABLE user (
    id       Integer primary key,
    name     String,
    fullname String
)
""")
    try:
        meta = MetaData()
        user_refrected = Table('user', meta, autoload=True,
                               autoload_with=engine_testaccount)
        assert user_refrected.c == ['user.id', 'user.name', 'user.fullname']
    finally:
        engine_testaccount.execute("""
DROP TABLE IF EXISTS user
""")


def test_inspect_column(engine_testaccount):
    """
    Tests Inspect
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount,
        metadata)
    try:
        inspector = inspect(engine_testaccount)
        all_table_names = inspector.get_table_names()
        assert 'users' in all_table_names
        assert 'addresses' in all_table_names

        columns_in_users = inspector.get_columns('users')

        assert columns_in_users[0]['autoincrement'], 'autoinrecment'
        assert columns_in_users[0]['default'] is None, 'default'
        assert columns_in_users[0]['name'] == 'id', 'name'
        assert columns_in_users[0]['primary_key'], 'primary key'

        assert not columns_in_users[1]['autoincrement'], 'autoinrecment'
        assert columns_in_users[1]['default'] is None, 'default'
        assert columns_in_users[1]['name'] == 'name', 'name'
        assert not columns_in_users[1]['primary_key'], 'primary key'

        assert not columns_in_users[2]['autoincrement'], 'autoinrecment'
        assert columns_in_users[2]['default'] is None, 'default'
        assert columns_in_users[2]['name'] == 'fullname', 'name'
        assert not columns_in_users[2]['primary_key'], 'primary key'

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_indexes(engine_testaccount):
    """
    Tests get indexes

    NOTE: Snowflake doesn't support indexes
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount,
        metadata)
    try:
        inspector = inspect(engine_testaccount)
        assert inspector.get_indexes("users") == []

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_primary_keys(engine_testaccount):
    """
    Tests get primary keys
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount,
        metadata)
    try:
        inspector = inspect(engine_testaccount)

        primary_keys = inspector.get_primary_keys('users')
        assert primary_keys['constrained_columns'] == ['id']

        primary_keys = inspector.get_primary_keys('addresses')
        assert primary_keys['constrained_columns'] == ['id']

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_foreign_keys(engine_testaccount):
    """
    Tests foreign keys
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables(
        engine_testaccount,
        metadata)

    try:
        inspector = inspect(engine_testaccount)
        foreign_keys = inspector.get_foreign_keys('addresses')
        assert foreign_keys[0]['constrained_columns'] == ['user_id']
    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_multile_column_primary_key(engine_testaccount):
    """
    Tests multicolumn primary key with and without autoincrement
    """
    metadata = MetaData()
    mytable = Table('mytable', metadata,
                    Column('gid',
                           Integer,
                           primary_key=True,
                           autoincrement=False),
                    Column('id',
                           Integer
                           , primary_key=True))

    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_mytable = inspector.get_columns('mytable')
        assert not columns_in_mytable[0]['autoincrement'], 'autoinrecment'
        assert columns_in_mytable[0]['default'] is None, 'default'
        assert columns_in_mytable[0]['name'] == 'gid', 'name'
        assert columns_in_mytable[0]['primary_key'], 'primary key'
        assert columns_in_mytable[1]['autoincrement'], 'autoinrecment'
        assert columns_in_mytable[1]['default'] is None, 'default'
        assert columns_in_mytable[1]['name'] == 'id', 'name'
        assert columns_in_mytable[1]['primary_key'], 'primary key'

        primary_keys = inspector.get_primary_keys('mytable')
        assert primary_keys['constrained_columns'] == ['gid', 'id']

    finally:
        mytable.drop(engine_testaccount)


def test_view_names(engine_testaccount):
    """
    Tests all views
    """
    inspector = inspect(engine_testaccount)

    information_schema_views = inspector.get_view_names(
        schema='information_schema')
    assert 'columns' in information_schema_views
    assert 'table_constraints' in information_schema_views


def test_view_definition(engine_testaccount):
    """
    Tests view definition
    """
    test_table_name = "test_table_sqlalchemy"
    test_view_name = "testview_sqlalchemy"
    engine_testaccount.execute("""
CREATE OR REPLACE TABLE {0} (
    id INTEGER,
    name STRING
)
""".format(test_table_name))
    sql = """
CREATE OR REPLACE VIEW {0} AS
SELECT * FROM {1} WHERE id > 10""".format(
        test_view_name, test_table_name)
    engine_testaccount.execute(text(sql).execution_options(
        autocommit=True))
    try:
        inspector = inspect(engine_testaccount)
        assert inspector.get_view_definition(test_view_name) == sql.strip()
        assert inspector.get_view_definition(test_view_name, 'testschema') == \
               sql.strip()
        assert inspector.get_view_names() == [test_view_name]
    finally:
        engine_testaccount.execute(
            "DROP TABLE IF EXISTS {0}".format(test_table_name))
        engine_testaccount.execute(
            "DROP VIEW IF EXISTS {0}".format(test_view_name))


@pytest.mark.skip("Temp table cannot be viewed for some reason")
def test_get_temp_table_names(engine_testaccount):
    num_of_temp_tables = 2
    temp_table_name = "temp_table"
    for idx in range(num_of_temp_tables):
        engine_testaccount.execute(text("""
CREATE TEMPORARY TABLE {0} (col1 integer, col2 string)
""".format(temp_table_name + str(idx))).execution_options(
            autocommit=True))
    for row in engine_testaccount.execute("SHOW TABLES"):
        print(row)
    try:
        inspector = inspect(engine_testaccount)
        temp_table_names = inspector.get_temp_table_names()
        assert len(temp_table_names) == num_of_temp_tables
    finally:
        pass


def test_create_table_with_schema(engine_testaccount):
    metadata = MetaData()
    Table('users', metadata,
          Column('id', Integer, Sequence('user_id_seq'),
                 primary_key=True),
          Column('name', String),
          Column('fullname', String),
          schema='public'
          )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        columns_in_users = inspector.get_columns('users', schema='public')
        assert columns_in_users is not None
    finally:
        metadata.drop_all(engine_testaccount)
