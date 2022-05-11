#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import os
import sys
import time
import uuid
from logging import getLogger

import pytest
import snowflake.connector
from parameters import CONNECTION_PARAMETERS
from snowflake.connector.compat import IS_WINDOWS
from snowflake.sqlalchemy import URL, dialect
from sqlalchemy import create_engine

if os.getenv('TRAVIS') == 'true':
    TEST_SCHEMA = 'TRAVIS_JOB_{}'.format(os.getenv('TRAVIS_JOB_ID'))
else:
    TEST_SCHEMA = (
            'sqlalchemy_tests_' + str(uuid.uuid4()).replace('-', '_'))


@pytest.fixture(scope='session')
def on_travis():
    return os.getenv('TRAVIS', '').lower() == 'true'


@pytest.fixture(scope='session')
def on_appveyor():
    return os.getenv('APPVEYOR', '').lower() == 'true'


@pytest.fixture(scope='session')
def on_public_ci(on_travis, on_appveyor):
    return on_travis or on_appveyor


def help():
    print("""Connection parameter must be specified in parameters.py,
    for example:
CONNECTION_PARAMETERS = {
    'account': 'testaccount',
    'user': 'user1',
    'password': 'test',
    'database': 'testdb',
    'schema': 'public',
}""")


logger = getLogger(__name__)

DEFAULT_PARAMETERS = {
    'account': '<account_name>',
    'user': '<user_name>',
    'password': '<password>',
    'database': '<database_name>',
    'schema': '<schema_name>',
    'protocol': 'https',
    'host': '<host>',
    'port': '443',
}


@pytest.fixture(scope='session')
def db_parameters():
    return get_db_parameters()


def get_db_parameters():
    """
    Sets the db connection parameters
    """
    ret = {}
    os.environ['TZ'] = 'UTC'
    if not IS_WINDOWS:
        time.tzset()
    for k, v in CONNECTION_PARAMETERS.items():
        ret[k] = v

    for k, v in DEFAULT_PARAMETERS.items():
        if k not in ret:
            ret[k] = v

    if 'account' in ret and ret['account'] == DEFAULT_PARAMETERS['account']:
        help()
        sys.exit(2)

    if 'host' in ret and ret['host'] == DEFAULT_PARAMETERS['host']:
        ret['host'] = ret['account'] + '.snowflakecomputing.com'

    # a unique table name
    ret['name'] = (
            'sqlalchemy_tests_' +
            str(uuid.uuid4())).replace('-', '_')
    ret['schema'] = TEST_SCHEMA

    # This reduces a chance to exposing password in test output.
    ret['a00'] = 'dummy parameter'
    ret['a01'] = 'dummy parameter'
    ret['a02'] = 'dummy parameter'
    ret['a03'] = 'dummy parameter'
    ret['a04'] = 'dummy parameter'
    ret['a05'] = 'dummy parameter'
    ret['a06'] = 'dummy parameter'
    ret['a07'] = 'dummy parameter'
    ret['a08'] = 'dummy parameter'
    ret['a09'] = 'dummy parameter'
    ret['a10'] = 'dummy parameter'
    ret['a11'] = 'dummy parameter'
    ret['a12'] = 'dummy parameter'
    ret['a13'] = 'dummy parameter'
    ret['a14'] = 'dummy parameter'
    ret['a15'] = 'dummy parameter'
    ret['a16'] = 'dummy parameter'

    return ret


def get_engine(user=None, password=None, account=None, schema=None):
    """
    Creates a connection using the parameters defined in JDBC connect string
    """
    ret = get_db_parameters()

    if user is not None:
        ret['user'] = user
    if password is not None:
        ret['password'] = password
    if account is not None:
        ret['account'] = account

    from sqlalchemy.pool import NullPool
    engine = create_engine(URL(
        user=ret['user'],
        password=ret['password'],
        host=ret['host'],
        port=ret['port'],
        database=ret['database'],
        schema=TEST_SCHEMA if not schema else schema,
        account=ret['account'],
        protocol=ret['protocol']
    ), poolclass=NullPool)

    return engine, ret


@pytest.fixture()
def engine_testaccount(request):
    engine, _ = get_engine()
    request.addfinalizer(engine.dispose)
    return engine


@pytest.fixture(scope='session', autouse=True)
def init_test_schema(request, db_parameters):
    ret = db_parameters
    with snowflake.connector.connect(
            user=ret['user'],
            password=ret['password'],
            host=ret['host'],
            port=ret['port'],
            database=ret['database'],
            account=ret['account'],
            protocol=ret['protocol']
    ) as con:
        con.cursor().execute(
            "CREATE SCHEMA IF NOT EXISTS {}".format(TEST_SCHEMA))

    def fin():
        ret1 = db_parameters
        with snowflake.connector.connect(
                user=ret1['user'],
                password=ret1['password'],
                host=ret1['host'],
                port=ret1['port'],
                database=ret1['database'],
                account=ret1['account'],
                protocol=ret1['protocol']
        ) as con1:
            con1.cursor().execute(
                "DROP SCHEMA IF EXISTS {}".format(TEST_SCHEMA))

    request.addfinalizer(fin)


@pytest.fixture(scope='session')
def sql_compiler():
    return lambda sql_command: str(sql_command.compile(dialect=dialect(),
                                                       compile_kwargs={'literal_binds': True,
                                                                       'deterministic': True})).replace('\n', '')
