#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2016 Snowflake Computing Inc. All right reserved.
#

import sys
import time
import uuid
from logging import getLogger

from parameters import (CONNECTION_PARAMETERS)

import os
import pytest
from snowflake.connector.compat import TO_UNICODE
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


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


@pytest.fixture()
def db_parameters():
    """
    Sets the db connection parameters
    """
    ret = {}
    os.environ['TZ'] = 'UTC'
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
        TO_UNICODE(uuid.uuid4())).replace('-', '_')
    ret['name_wh'] = ret['name'] + 'wh'

    return ret


def get_engine(user=None, password=None, account=None):
    """
    Creates a connection using the parameters defined in JDBC connect string
    """
    ret = db_parameters()

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
        schema=ret['schema'],
        account=ret['account'],
        protocol=ret['protocol']
    ), poolclass=NullPool)

    return engine


@pytest.fixture()
def engine_testaccount(request):
    engine = get_engine()

    def fin():
        engine.dispose()  # close when done

    request.addfinalizer(fin)
    return engine
