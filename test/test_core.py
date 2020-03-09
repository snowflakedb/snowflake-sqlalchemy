#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#
import os
import random
import re
import string
import time

import pytest
from conftest import get_engine
from mock import patch
from parameters import CONNECTION_PARAMETERS
from snowflake.connector import ProgrammingError, connect
from snowflake.sqlalchemy import URL, MergeInto, dialect
from sqlalchemy import (
    REAL,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
    Sequence,
    String,
    Table,
    create_engine,
    dialects,
    inspect,
    text,
)
from sqlalchemy.sql import and_, not_, or_, select

try:
    from parameters import (CONNECTION_PARAMETERS2)
except ImportError:
    CONNECTION_PARAMETERS2 = CONNECTION_PARAMETERS

THIS_DIR = os.path.dirname(os.path.realpath(__file__))


def _create_users_addresses_tables(engine_testaccount, metadata, fk=None):
    users = Table('users', metadata,
                  Column('id', Integer, Sequence('user_id_seq'),
                         primary_key=True),
                  Column('name', String),
                  Column('fullname', String),
                  )

    addresses = Table('addresses', metadata,
                      Column('id', Integer, Sequence('address_id_seq'),
                             primary_key=True),
                      Column('user_id', None,
                             ForeignKey('users.id', name=fk)),
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

    Snowflake connect string supports account name as a replacement of
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

    engine = create_engine(URL(
        user=CONNECTION_PARAMETERS2['user'],
        password=CONNECTION_PARAMETERS2['password'],
        account=CONNECTION_PARAMETERS2['account'],
        warehouse='testwh'
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
    users, addresses = _create_users_addresses_tables(
        engine_testaccount, metadata)

    conn = engine_testaccount.connect()
    try:
        # inserts data with an implicitly generated id
        ins = users.insert().values(name='jack', fullname='Jack Jones')
        results = engine_testaccount.execute(ins)
        assert results.inserted_primary_key == [1], 'sequence value'
        results.close()

        # inserts data with the given id
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
        assert str(users.c.name == None)  # NOQA
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
                      (addresses.c.email_address == 'wendy@aol.com') |
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
               'ON addresses.email_address LIKE users.name || :name_1'

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
        conn.close()
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
        user_reflected = Table('user', meta, autoload=True,
                               autoload_with=engine_testaccount)
        assert user_reflected.c == ['user.id', 'user.name', 'user.fullname']
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

        assert columns_in_users[0]['autoincrement'], 'autoincrement'
        assert columns_in_users[0]['default'] is None, 'default'
        assert columns_in_users[0]['name'] == 'id', 'name'
        assert columns_in_users[0]['primary_key'], 'primary key'

        assert not columns_in_users[1]['autoincrement'], 'autoincrement'
        assert columns_in_users[1]['default'] is None, 'default'
        assert columns_in_users[1]['name'] == 'name', 'name'
        assert not columns_in_users[1]['primary_key'], 'primary key'

        assert not columns_in_users[2]['autoincrement'], 'autoincrement'
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

        primary_keys = inspector.get_pk_constraint('users')
        assert primary_keys['constrained_columns'] == ['id']

        primary_keys = inspector.get_pk_constraint('addresses')
        assert primary_keys['constrained_columns'] == ['id']

    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


def test_get_foreign_keys(engine_testaccount):
    """
    Tests foreign keys
    """
    metadata = MetaData()
    fk_name = 'fk_users_id_from_addresses'
    users, addresses = _create_users_addresses_tables(
        engine_testaccount,
        metadata, fk=fk_name)

    try:
        inspector = inspect(engine_testaccount)
        foreign_keys = inspector.get_foreign_keys('addresses')
        assert foreign_keys[0]['name'] == fk_name
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
                           Integer,
                           primary_key=True,
                           autoincrement=True))

    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_mytable = inspector.get_columns('mytable')
        assert not columns_in_mytable[0]['autoincrement'], 'autoincrement'
        assert columns_in_mytable[0]['default'] is None, 'default'
        assert columns_in_mytable[0]['name'] == 'gid', 'name'
        assert columns_in_mytable[0]['primary_key'], 'primary key'
        assert columns_in_mytable[1]['autoincrement'], 'autoincrement'
        assert columns_in_mytable[1]['default'] is None, 'default'
        assert columns_in_mytable[1]['name'] == 'id', 'name'
        assert columns_in_mytable[1]['primary_key'], 'primary key'

        primary_keys = inspector.get_pk_constraint('mytable')
        assert primary_keys['constrained_columns'] == ['gid', 'id']

    finally:
        mytable.drop(engine_testaccount)


def test_create_table_with_cluster_by(engine_testaccount):
    # Test case for https://github.com/snowflakedb/snowflake-sqlalchemy/pull/14
    metadata = MetaData()
    user = Table('clustered_user', metadata,
                 Column('Id', Integer, primary_key=True),
                 Column('name', String),
                 snowflake_clusterby=['Id', 'name'])
    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_table = inspector.get_columns('clustered_user')
        assert columns_in_table[0]['name'] == 'Id', 'name'
    finally:
        user.drop(engine_testaccount)


def test_view_names(engine_testaccount):
    """
    Tests all views
    """
    inspector = inspect(engine_testaccount)

    information_schema_views = inspector.get_view_names(
        schema='information_schema')
    assert 'columns' in information_schema_views
    assert 'table_constraints' in information_schema_views


def test_view_definition(engine_testaccount, db_parameters):
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
        assert inspector.get_view_definition(test_view_name,
                                             db_parameters['schema']) == \
               sql.strip()
        assert inspector.get_view_names() == [test_view_name]
    finally:
        engine_testaccount.execute(text(
            "DROP TABLE IF EXISTS {0}".format(test_table_name)))
        engine_testaccount.execute(text(
            "DROP VIEW IF EXISTS {0}".format(test_view_name)))


def test_view_comment_reading(engine_testaccount, db_parameters):
    """
    Tests reading a comment from a view once it's defined
    """
    test_table_name = "test_table_sqlalchemy"
    test_view_name = "testview_sqlalchemy"
    engine_testaccount.execute("""
CREATE OR REPLACE TABLE {} (
    id INTEGER,
    name STRING
)
""".format(test_table_name))
    sql = """
CREATE OR REPLACE VIEW {} AS
SELECT * FROM {} WHERE id > 10""".format(
        test_view_name, test_table_name)
    engine_testaccount.execute(text(sql).execution_options(
        autocommit=True))
    comment_text = "hello my viewing friends"
    sql = "COMMENT ON VIEW {} IS '{}';".format(
        test_view_name, comment_text)
    engine_testaccount.execute(text(sql).execution_options(
        autocommit=True))
    try:
        inspector = inspect(engine_testaccount)
        # NOTE: sqlalchemy doesn't have a way to get view comments specifically,
        # but the code to get table comments should work for views too
        assert inspector.get_table_comment(test_view_name) == {'text': comment_text}
        assert inspector.get_table_comment(test_table_name) == {'text': None}
        assert str(inspector.get_columns(test_table_name)) == str(inspector.get_columns(test_view_name))
    finally:
        engine_testaccount.execute(text(
            "DROP TABLE IF EXISTS {0}".format(test_table_name)))
        engine_testaccount.execute(text(
            "DROP VIEW IF EXISTS {0}".format(test_view_name)))


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


def test_create_table_with_schema(engine_testaccount, db_parameters):
    metadata = MetaData()
    new_schema = db_parameters['schema'] + "_NEW"
    engine_testaccount.execute(text(
        "CREATE OR REPLACE SCHEMA \"{0}\"".format(new_schema)))
    Table('users', metadata,
          Column('id', Integer, Sequence('user_id_seq'),
                 primary_key=True),
          Column('name', String),
          Column('fullname', String),
          schema=new_schema
          )
    metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        columns_in_users = inspector.get_columns('users', schema=new_schema)
        assert columns_in_users is not None
    finally:
        metadata.drop_all(engine_testaccount)
        engine_testaccount.execute(
            text("DROP SCHEMA IF EXISTS \"{0}\"".format(new_schema)))


@pytest.mark.skipif(os.getenv("SNOWFLAKE_GCP") is not None, reason="PUT and GET is not supported for GCP yet")
def test_copy(engine_testaccount):
    """
    COPY must be in a transaction
    """
    metadata = MetaData()
    users, addresses = _create_users_addresses_tables_without_sequence(
        engine_testaccount,
        metadata)

    try:
        engine_testaccount.execute(
            "PUT file://{file_name} @%users".format(
                file_name=os.path.join(THIS_DIR, "data", "users.txt")))
        engine_testaccount.execute("COPY INTO users")
        results = engine_testaccount.execute("SELECT * FROM USERS").fetchall()
        assert results is not None and len(results) > 0
    finally:
        addresses.drop(engine_testaccount)
        users.drop(engine_testaccount)


@pytest.mark.skip("""
No transaction works yet in the core API. Use orm API or Python Connector
directly if needed at the moment.
Note Snowflake DB supports DML transaction natively, but we have not figured out
how to integrate with SQLAlchemy core API yet.
""")
def test_transaction(engine_testaccount, db_parameters):
    engine_testaccount.execute(text("""
CREATE TABLE {0} (c1 number)""".format(db_parameters['name'])))
    trans = engine_testaccount.connect().begin()
    try:
        engine_testaccount.execute(text("""
INSERT INTO {0} VALUES(123)
        """.format(db_parameters['name'])))
        trans.commit()
        engine_testaccount.execute(text("""
INSERT INTO {0} VALUES(456)
        """.format(db_parameters['name'])))
        trans.rollback()
        results = engine_testaccount.execute("""
SELECT * FROM {0}
""".format(db_parameters['name'])).fetchall()
        assert results == [(123,)]
    finally:
        engine_testaccount.execute(text("""
DROP TABLE IF EXISTS {0}
""".format(db_parameters['name'])))


def test_get_schemas(engine_testaccount):
    """
    Tests get schemas from inspect.

    Although the method get_schema_names is not part of DefaultDialect,
    inspect() may call the method if exists.
    """
    inspector = inspect(engine_testaccount)

    schemas = inspector.get_schema_names()
    assert 'information_schema' in schemas


def test_column_metadata(engine_testaccount):
    from sqlalchemy.ext.declarative import declarative_base

    Base = declarative_base()

    class Appointment(Base):
        __tablename__ = 'appointment'
        id = Column(Numeric(38, 3), primary_key=True)
        string_with_len = Column(String(100))
        binary_data = Column(LargeBinary)
        real_data = Column(REAL)

    Base.metadata.create_all(engine_testaccount)

    metadata = Base.metadata

    t = Table('appointment', metadata)

    inspector = inspect(engine_testaccount)
    inspector.reflecttable(t, None)
    assert str(t.columns['id'].type) == 'DECIMAL(38, 3)'
    assert str(t.columns['string_with_len'].type) == 'VARCHAR(100)'
    assert str(t.columns['binary_data'].type) == 'BINARY'
    assert str(t.columns['real_data'].type) == 'FLOAT'


def _get_engine_with_columm_metadata_cache(
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

    from sqlalchemy.pool import NullPool
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
        cache_column_metadata=True,
    ), poolclass=NullPool)

    return engine


def test_many_table_column_metadta(db_parameters):
    """
    Get dozens of table metadata with column metadata cache.

    cache_column_metadata=True will cache all column metadata for all tables
    in the schema.
    """
    engine = _get_engine_with_columm_metadata_cache(db_parameters)
    RE_SUFFIX_NUM = re.compile(r'.*(\d+)$')
    metadata = MetaData()
    total_objects = 10
    for idx in range(total_objects):
        Table('mainusers' + str(idx), metadata,
              Column('id' + str(idx), Integer, Sequence('user_id_seq'),
                     primary_key=True),
              Column('name' + str(idx), String),
              Column('fullname', String),
              Column('password', String)
              )
        Table('mainaddresses' + str(idx), metadata,
              Column('id' + str(idx), Integer, Sequence('address_id_seq'),
                     primary_key=True),
              Column('user_id' + str(idx), None,
                     ForeignKey('mainusers' + str(idx) + '.id' + str(idx))),
              Column('email_address' + str(idx), String, nullable=False)
              )
    metadata.create_all(engine)

    inspector = inspect(engine)
    cnt = 0
    schema = inspector.default_schema_name
    for table_name in inspector.get_table_names(schema):
        m = RE_SUFFIX_NUM.match(table_name)
        if m:
            suffix = m.group(1)
            cs = inspector.get_columns(table_name, schema)
            if table_name.startswith("mainusers"):
                assert len(cs) == 4
                assert cs[1]['name'] == 'name' + suffix
                cnt += 1
            elif table_name.startswith("mainaddresses"):
                assert len(cs) == 3
                assert cs[2]['name'] == 'email_address' + suffix
                cnt += 1
            ps = inspector.get_pk_constraint(table_name, schema)
            if table_name.startswith("mainusers"):
                assert ps['constrained_columns'] == ['id' + suffix]
            elif table_name.startswith("mainaddresses"):
                assert ps['constrained_columns'] == ['id' + suffix]
            fs = inspector.get_foreign_keys(table_name, schema)
            if table_name.startswith("mainusers"):
                assert len(fs) == 0
            elif table_name.startswith("mainaddresses"):
                assert len(fs) == 1
                assert fs[0]['constrained_columns'] == ['user_id' + suffix]
                assert fs[0]['referred_table'] == 'mainusers' + suffix

    assert cnt == total_objects * 2, 'total number of test objects'


def test_cache_time(engine_testaccount, db_parameters):
    """Check whether Inspector cache is working"""
    # Set up necessary tables
    metadata = MetaData()
    total_objects = 10
    for idx in range(total_objects):
        Table('mainusers' + str(idx), metadata,
              Column('id' + str(idx), Integer, Sequence('user_id_seq'),
                     primary_key=True),
              Column('name' + str(idx), String),
              Column('fullname', String),
              Column('password', String)
              )
        Table('mainaddresses' + str(idx), metadata,
              Column('id' + str(idx), Integer, Sequence('address_id_seq'),
                     primary_key=True),
              Column('user_id' + str(idx), None,
                     ForeignKey('mainusers' + str(idx) + '.id' + str(idx))),
              Column('email_address' + str(idx), String, nullable=False)
              )
    metadata.create_all(engine_testaccount)
    inspector = inspect(engine_testaccount)
    schema = db_parameters['schema']

    def harass_inspector():
        for table_name in inspector.get_table_names(schema):
            inspector.get_columns(table_name, schema)
            inspector.get_pk_constraint(table_name, schema)
            inspector.get_foreign_keys(table_name, schema)

    outcome = False
    # Allow up to 5 times for the speed test to pass to avoid flaky test
    for _ in range(5):
        # Python 2.7 has no timeit.timeit with globals and locals parameters
        s_time = time.time()
        harass_inspector()
        m_time = time.time()
        harass_inspector()
        time2 = time.time() - m_time
        time1 = m_time - s_time
        print("Ran inspector through tables twice, times:\n\tfirst: {0}\n\tsecond: {1}".format(time1, time2))
        if time2 < time1 * 0.01:
            outcome = True
            break
        else:
            # Reset inspector to reset cache
            inspector = inspect(engine_testaccount)
    metadata.drop_all(engine_testaccount)
    assert outcome


@pytest.mark.timeout(15)
def test_region():
    from sqlalchemy import create_engine
    engine = create_engine(URL(
        user='testuser',
        password='testpassword',
        account='testaccount',
        region='eu-central-1',
        login_timeout=5
    ))
    try:
        engine.execute('select current_version()').fetchone()
        pytest.fail('should not run')
    except Exception as ex:
        assert ex.orig.errno == 250001
        assert 'Failed to connect to DB' in ex.orig.msg
        assert 'testaccount.eu-central-1.snowflakecomputing.com' in ex.orig.msg


@pytest.mark.timeout(15)
def test_azure():
    from sqlalchemy import create_engine
    engine = create_engine(URL(
        user='testuser',
        password='testpassword',
        account='testaccount',
        region='east-us-2.azure',
        login_timeout=5
    ))
    try:
        engine.execute('select current_version()').fetchone()
        pytest.fail('should not run')
    except Exception as ex:
        assert ex.orig.errno == 250001
        assert 'Failed to connect to DB' in ex.orig.msg
        assert 'testaccount.east-us-2.azure.snowflakecomputing.com' in \
               ex.orig.msg


def test_load_dialect():
    """
    Test loading Snowflake SQLAlchemy dialect class
    """
    assert isinstance(dialects.registry.load('snowflake')(), dialect)


@pytest.mark.parametrize('conditional_flag', [True, False])
@pytest.mark.parametrize('update_flag,insert_flag,delete_flag', [
    (True, False, False),
    (False, True, False),
    (False, False, True),
    (False, True, True),
    (True, True, False)])
def test_upsert(engine_testaccount, update_flag, insert_flag, delete_flag, conditional_flag):
    meta = MetaData()
    users = Table('users', meta,
                  Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
                  Column('name', String),
                  Column('fullname', String))
    onboarding_users = Table('onboarding_users', meta,
                             Column('id', Integer, Sequence('new_user_id_seq'), primary_key=True),
                             Column('name', String),
                             Column('fullname', String),
                             Column('delete', Boolean))
    meta.create_all(engine_testaccount)
    conn = engine_testaccount.connect()
    try:
        conn.execute(users.insert(), [
            {'id': 1, 'name': 'mark', 'fullname': 'Mark Keller'},
            {'id': 4, 'name': 'luke', 'fullname': 'Luke Lorimer'},
            {'id': 2, 'name': 'amanda', 'fullname': 'Amanda Harris'}])
        conn.execute(onboarding_users.insert(), [
            {'id': 2, 'name': 'amanda', 'fullname': 'Amanda Charlotte Harris', 'delete': True},
            {'id': 3, 'name': 'jim', 'fullname': 'Jim Wang', 'delete': False},
            {'id': 4, 'name': 'lukas', 'fullname': 'Lukas Lorimer', 'delete': False},
            {'id': 5, 'name': 'andras', 'fullname': None, 'delete': False}
        ])

        merge = MergeInto(users, onboarding_users, users.c.id == onboarding_users.c.id)
        if update_flag:
            clause = merge.when_matched_then_update().values(name=onboarding_users.c.name,
                                                             fullname=onboarding_users.c.fullname)
            if conditional_flag:
                clause.where(onboarding_users.c.name != 'amanda')
        if insert_flag:
            clause = merge.when_not_matched_then_insert().values(
                id=onboarding_users.c.id,
                name=onboarding_users.c.name,
                fullname=onboarding_users.c.fullname,
            )
            if conditional_flag:
                clause.where(onboarding_users.c.fullname != None)  # NOQA
        if delete_flag:
            clause = merge.when_matched_then_delete()
            if conditional_flag:
                clause.where(onboarding_users.c.delete == True)  # NOQA

        conn.execute(merge)
        users_tuples = {tuple(row) for row in conn.execute(select([users]))}
        onboarding_users_tuples = {tuple(row) for row in conn.execute(select([onboarding_users]))}
        expected_users = {
            (1, 'mark', 'Mark Keller'),
            (2, 'amanda', 'Amanda Harris'),
            (4, 'luke', 'Luke Lorimer')
        }
        if update_flag:
            if not conditional_flag:
                expected_users.remove((2, 'amanda', 'Amanda Harris'))
                expected_users.add((2, 'amanda', 'Amanda Charlotte Harris'))
            expected_users.remove((4, 'luke', 'Luke Lorimer'))
            expected_users.add((4, 'lukas', 'Lukas Lorimer'))
        elif delete_flag:
            if not conditional_flag:
                expected_users.remove((4, 'luke', 'Luke Lorimer'))
            expected_users.remove((2, 'amanda', 'Amanda Harris'))
        if insert_flag:
            if not conditional_flag:
                expected_users.add((5, 'andras', None))
            expected_users.add((3, 'jim', 'Jim Wang'))
        expected_onboarding_users = {
            (2, 'amanda', 'Amanda Charlotte Harris', True),
            (3, 'jim', 'Jim Wang', False),
            (4, 'lukas', 'Lukas Lorimer', False),
            (5, 'andras', None, False)
        }
        assert users_tuples == expected_users
        assert onboarding_users_tuples == expected_onboarding_users
    finally:
        conn.close()
        users.drop(engine_testaccount)
        onboarding_users.drop(engine_testaccount)


def test_deterministic_merge_into(sql_compiler):
    meta = MetaData()
    users = Table('users', meta,
                  Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
                  Column('name', String),
                  Column('fullname', String))
    onboarding_users = Table('onboarding_users', meta,
                             Column('id', Integer, Sequence('new_user_id_seq'), primary_key=True),
                             Column('name', String),
                             Column('fullname', String),
                             Column('delete', Boolean))
    merge = MergeInto(users, onboarding_users, users.c.id == onboarding_users.c.id)
    merge.when_matched_then_update().values(name=onboarding_users.c.name,
                                            fullname=onboarding_users.c.fullname)
    merge.when_not_matched_then_insert().values(
        id=onboarding_users.c.id,
        name=onboarding_users.c.name,
        fullname=onboarding_users.c.fullname,
    ).where(onboarding_users.c.fullname != None)  # NOQA
    assert sql_compiler(merge) == "MERGE INTO users USING onboarding_users ON users.id = onboarding_users.id " \
                                  "WHEN MATCHED THEN UPDATE SET fullname = onboarding_users.fullname, " \
                                  "name = onboarding_users.name WHEN NOT MATCHED AND onboarding_users.fullname " \
                                  "IS NOT NULL THEN INSERT (fullname, id, name) VALUES (onboarding_users.fullname, " \
                                  "onboarding_users.id, onboarding_users.name)"


def test_comments(engine_testaccount):
    """Tests strictly reading column comment through SQLAlchemy"""
    table_name = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
    try:
        engine_testaccount.execute("create table public.{} (\"col1\" text);".format(table_name))
        engine_testaccount.execute("alter table public.{} alter \"col1\" comment 'this is my comment'".format(table_name))
        engine_testaccount.execute("select comment from information_schema.columns where table_name='{}'".format(table_name)).fetchall()
        inspector = inspect(engine_testaccount)
        columns = inspector.get_columns(table_name, schema='PUBLIC')
        assert columns[0].get('comment') == u'this is my comment'
    finally:
        engine_testaccount.execute("drop table public.{}".format(table_name))


def test_comment_sqlalchemy(db_parameters, engine_testaccount, on_public_ci):
    """Testing adding/reading column and table comments through SQLAlchemy"""
    new_schema = db_parameters['schema'] + '2'
    # Use same table name in 2 different schemas to make sure comment retrieval works properly
    table_name = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
    table_comment1 = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
    column_comment1 = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
    table_comment2 = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
    column_comment2 = ''.join(random.choice(string.ascii_uppercase) for _ in range(10))
    engine2, _ = get_engine(schema=new_schema)
    con2 = None
    if not on_public_ci:
        con2 = engine2.connect()
        con2.execute("CREATE SCHEMA IF NOT EXISTS {0}".format(new_schema))
    inspector = inspect(engine_testaccount)
    metadata1 = MetaData()
    metadata2 = MetaData()
    mytable1 = Table(table_name,
                     metadata1,
                     Column("tstamp", DateTime, comment=column_comment1),
                     comment=table_comment1)
    mytable2 = Table(table_name,
                     metadata2,
                     Column("tstamp", DateTime, comment=column_comment2),
                     comment=table_comment2)

    metadata1.create_all(engine_testaccount, tables=[mytable1])
    if not on_public_ci:
        metadata2.create_all(engine2, tables=[mytable2])

    try:
        assert inspector.get_columns(table_name)[0]['comment'] == column_comment1
        assert inspector.get_table_comment(table_name)['text'] == table_comment1
        if not on_public_ci:
            assert inspector.get_columns(table_name, schema=new_schema)[0]['comment'] == column_comment2
            assert inspector.get_table_comment(
                table_name,
                schema=new_schema.upper()  # Note: since did not quote schema name it was uppercase'd
            )['text'] == table_comment2
    finally:
        mytable1.drop(engine_testaccount)
        if not on_public_ci:
            mytable2.drop(engine2)
            con2.execute("DROP SCHEMA IF EXISTS {0}".format(new_schema))
            con2.close()
        engine2.dispose()


def test_special_schema_character(db_parameters, on_public_ci):
    """Make sure we decode special characters correctly"""
    if on_public_ci:
        pytest.skip("Public CIs cannot create Schemas and Databases")
    # Constants
    database = "a/b/c"  # "'/'.join([choice(ascii_lowercase) for _ in range(3)])
    schema = "d/e/f"  # '/'.join([choice(ascii_lowercase) for _ in range(3)])
    # Setup
    options = dict(**db_parameters)
    conn = connect(**options)
    conn.cursor().execute("CREATE OR REPLACE DATABASE \"{0}\"".format(database))
    conn.cursor().execute("CREATE OR REPLACE SCHEMA \"{0}\"".format(schema))
    conn.close()
    # Test
    options.update({'database': '"' + database + '"',
                    'schema': '"' + schema + '"'})
    sf_conn = connect(**options)
    sf_connection = [res for res in sf_conn.cursor().execute("select current_database(), "
                                                             "current_schema();")]
    sa_conn = create_engine(URL(**options)).connect()
    sa_connection = [res for res in sa_conn.execute("select current_database(), "
                                                    "current_schema();")]
    sa_conn.close()
    sf_conn.close()
    # Teardown
    conn = connect(**options)
    conn.cursor().execute("DROP DATABASE IF EXISTS \"{0}\"".format(database))
    conn.close()
    assert [(database, schema)] == sf_connection == sa_connection


def test_autoincrement(engine_testaccount):
    metadata = MetaData()
    users = Table('users', metadata,
               Column('uid', Integer, Sequence('id_seq'), primary_key=True),
               Column('name', String(39)))

    try:
        users.create(engine_testaccount)

        connection = engine_testaccount.connect()
        connection.execute(users.insert(), [{'name': 'sf1'}])

        assert connection.execute(select([users])).fetchall() == [
            (1, 'sf1')
        ]

        connection.execute(users.insert(), {'name': 'sf2'}, {'name': 'sf3'})
        assert connection.execute(select([users])).fetchall() == [
            (1, 'sf1'),
            (2, 'sf2'),
            (3, 'sf3')
        ]

        connection.execute(users.insert(), {'name': 'sf4'})
        assert connection.execute(select([users])).fetchall() == [
            (1, 'sf1'),
            (2, 'sf2'),
            (3, 'sf3'),
            (4, 'sf4')
        ]

        seq = Sequence('id_seq')
        nextid = connection.execute(seq)
        connection.execute(users.insert(), [{'uid': nextid, 'name': 'sf5'}])
        assert connection.execute(select([users])).fetchall() == [
            (1, 'sf1'),
            (2, 'sf2'),
            (3, 'sf3'),
            (4, 'sf4'),
            (5, 'sf5')
        ]
    finally:
        users.drop(engine_testaccount)


def test_get_too_many_columns(engine_testaccount, db_parameters):
    """Check whether Inspector cache is working, when there are too many column to cache whole schema's columns"""
    # Set up necessary tables
    metadata = MetaData()
    total_objects = 10
    for idx in range(total_objects):
        Table('mainuserss' + str(idx), metadata,
              Column('id' + str(idx), Integer, Sequence('user_id_seq'),
                     primary_key=True),
              Column('name' + str(idx), String),
              Column('fullname', String),
              Column('password', String)
              )
        Table('mainaddressess' + str(idx), metadata,
              Column('id' + str(idx), Integer, Sequence('address_id_seq'),
                     primary_key=True),
              Column('user_id' + str(idx), None,
                     ForeignKey('mainuserss' + str(idx) + '.id' + str(idx))),
              Column('email_address' + str(idx), String, nullable=False)
              )
    metadata.create_all(engine_testaccount)
    inspector = inspect(engine_testaccount)
    schema = db_parameters['schema']

    # Emulate error
    with patch.object(inspector.dialect, '_get_schema_columns', return_value=None) as mock_method:
        def harass_inspector():
            for table_name in inspector.get_table_names(schema):
                column_metadata = inspector.get_columns(table_name, schema)
                inspector.get_pk_constraint(table_name, schema)
                inspector.get_foreign_keys(table_name, schema)
                assert 3 <= len(column_metadata) <= 4  # Either one of the tables should have 3 or 4 columns

        outcome = False
        # Allow up to 5 times for the speed test to pass to avoid flaky test
        for _ in range(5):
            # Python 2.7 has no timeit.timeit with globals and locals parameters
            s_time = time.time()
            harass_inspector()
            m_time = time.time()
            harass_inspector()
            time2 = time.time() - m_time
            time1 = m_time - s_time
            print("Ran inspector through tables twice, times:\n\tfirst: {0}\n\tsecond: {1}".format(time1, time2))
            if time2 < time1 * 0.01:
                outcome = True
                break
            else:
                # Reset inspector to reset cache
                inspector = inspect(engine_testaccount)
        metadata.drop_all(engine_testaccount)
        assert mock_method.call_count > 0  # Make sure we actually mocked the issue happening
        assert outcome


def test_too_many_columns_detection(engine_testaccount, db_parameters):
    """This tests whether a too many column error actually triggers the more granular table version"""
    # Set up a single table
    metadata = MetaData()
    Table('users', metadata,
          Column('id', Integer, Sequence('user_id_seq'),
                 primary_key=True),
          Column('name', String),
          Column('fullname', String),
          Column('password', String)
          )
    metadata.create_all(engine_testaccount)
    inspector = inspect(engine_testaccount)
    # Do test
    original_execute = inspector.bind.execute

    def mock_helper(command, *args, **kwargs):
        if '_get_schema_columns' in command:
            raise ProgrammingError("Information schema query returned too much data. Please repeat query with more "
                                   "selective predicates.", 90030)
        else:
            return original_execute(command, *args, **kwargs)

    with patch.object(inspector.bind, 'execute', side_effect=mock_helper):
        column_metadata = inspector.get_columns('users', db_parameters['schema'])
    assert len(column_metadata) == 4
    # Clean up
    metadata.drop_all(engine_testaccount)


def test_empty_comments(engine_testaccount):
    """Test that no comment returns None"""
    table_name = ''.join(random.choice(string.ascii_uppercase) for _ in range(5))
    try:
        engine_testaccount.execute("create table public.{} (\"col1\" text);".format(table_name))
        engine_testaccount.execute("select comment from information_schema.columns where table_name='{}'".format(table_name)).fetchall()
        inspector = inspect(engine_testaccount)
        columns = inspector.get_columns(table_name, schema='PUBLIC')
        assert inspector.get_table_comment(table_name, schema='PUBLIC') == {'text': None}
        assert all([c['comment'] is None for c in columns])
    finally:
        engine_testaccount.execute("drop table public.{}".format(table_name))
