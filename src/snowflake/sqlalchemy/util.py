#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import exc

from snowflake.connector.compat import IS_STR
from snowflake.connector.connection import SnowflakeConnection

from ._constants import (
    APPLICATION_NAME,
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)


def _url(**db_parameters):
    """
    Composes a SQLAlchemy connect string from the given database connection
    parameters.

    Password containing special characters (e.g., '@', '%') need to be encoded to be parsed correctly.
    Unescaped password containing special characters might lead to authentication failure.
    Please follow the instructions to encode the password:
    https://github.com/snowflakedb/snowflake-sqlalchemy#escaping-special-characters-such-as---signs-in-passwords
    """
    specified_parameters = []
    if "account" not in db_parameters:
        raise exc.ArgumentError("account parameter must be specified.")

    if "host" in db_parameters:
        ret = "snowflake://{user}:{password}@{host}:{port}/".format(
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            host=db_parameters["host"],
            port=db_parameters["port"] if "port" in db_parameters else 443,
        )
        specified_parameters += ["user", "password", "host", "port"]
    elif "region" not in db_parameters:
        ret = "snowflake://{user}:{password}@{account}/".format(
            account=db_parameters["account"],
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
        )
        specified_parameters += ["user", "password", "account"]
    else:
        ret = "snowflake://{user}:{password}@{account}.{region}/".format(
            account=db_parameters["account"],
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            region=db_parameters["region"],
        )
        specified_parameters += ["user", "password", "account", "region"]

    if "database" in db_parameters:
        ret += quote_plus(db_parameters["database"])
        specified_parameters += ["database"]
        if "schema" in db_parameters:
            ret += "/" + quote_plus(db_parameters["schema"])
            specified_parameters += ["schema"]
    elif "schema" in db_parameters:
        raise exc.ArgumentError("schema cannot be specified without database")

    def sep(is_first_parameter):
        return "?" if is_first_parameter else "&"

    is_first_parameter = True
    for p in sorted(db_parameters.keys()):
        v = db_parameters[p]
        if p not in specified_parameters:
            encoded_value = quote_plus(v) if IS_STR(v) else str(v)
            ret += sep(is_first_parameter) + p + "=" + encoded_value
            is_first_parameter = False
    return ret


def _set_connection_interpolate_empty_sequences(
    dbapi_connection: SnowflakeConnection, flag: bool
) -> None:
    """set the _interpolate_empty_sequences config of the underlying connection"""
    if hasattr(dbapi_connection, "driver_connection"):
        # _dbapi_connection is a _ConnectionFairy which proxies raw SnowflakeConnection
        dbapi_connection.driver_connection._interpolate_empty_sequences = flag
    else:
        # _dbapi_connection is a raw SnowflakeConnection
        dbapi_connection._interpolate_empty_sequences = flag


def _update_connection_application_name(**conn_kwargs: Any) -> Any:
    if PARAM_APPLICATION not in conn_kwargs:
        conn_kwargs[PARAM_APPLICATION] = APPLICATION_NAME
    if PARAM_INTERNAL_APPLICATION_NAME not in conn_kwargs:
        conn_kwargs[PARAM_INTERNAL_APPLICATION_NAME] = APPLICATION_NAME
    if PARAM_INTERNAL_APPLICATION_VERSION not in conn_kwargs:
        conn_kwargs[PARAM_INTERNAL_APPLICATION_VERSION] = SNOWFLAKE_SQLALCHEMY_VERSION
    return conn_kwargs
