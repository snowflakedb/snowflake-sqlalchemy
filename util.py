#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from sqlalchemy import exc

from snowflake.connector.compat import (PY2, IS_STR)

if PY2:
    from urllib import quote
else:
    from urllib.parse import quote


def _url(**db_parameters):
    """
    Composes a SQLAlchemy connect string from the given database connection
    parameters.
    """
    specified_parameters = []
    if 'account' not in db_parameters:
        raise exc.ArgumentError("account parameter must be specified.")

    if 'host' in db_parameters:
        ret = 'snowflake://{user}:{password}@{host}:{port}/'.format(
            user=db_parameters.get('user', ''),
            password=quote(db_parameters.get('password', ''), safe=''),
            host=db_parameters['host'],
            port=db_parameters['port'] if 'port' in db_parameters else 443,
        )
        specified_parameters += ['user', 'password', 'host', 'port']
    elif 'region' not in db_parameters:
        ret = 'snowflake://{user}:{password}@{account}/'.format(
            account=db_parameters['account'],
            user=db_parameters.get('user', ''),
            password=quote(db_parameters.get('password', ''), safe=''),
        )
        specified_parameters += ['user', 'password', 'account']
    else:
        ret = 'snowflake://{user}:{password}@{account}.{region}/'.format(
            account=db_parameters['account'],
            user=db_parameters.get('user', ''),
            password=quote(db_parameters.get('password', ''), safe=''),
            region=db_parameters['region'],
        )
        specified_parameters += ['user', 'password', 'account', 'region']

    if 'database' in db_parameters:
        ret += quote(db_parameters['database'], safe='')
        specified_parameters += ['database']
        if 'schema' in db_parameters:
            ret += '/' + quote(db_parameters['schema'], safe='')
            specified_parameters += ['schema']
    elif 'schema' in db_parameters:
        raise exc.ArgumentError("schema cannot be specified without database")

    def sep(is_first_parameter):
        return '?' if is_first_parameter else '&'

    is_first_parameter = True
    for p in sorted(db_parameters.keys()):
        v = db_parameters[p]
        if p not in specified_parameters:
            encoded_value = quote(v, safe='') if IS_STR(v) else str(v)
            ret += sep(is_first_parameter) + p + '=' + encoded_value
            is_first_parameter = False
    return ret
