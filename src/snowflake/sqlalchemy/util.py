#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re
from itertools import chain
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy import exc
from sqlalchemy.sql.base import _expand_cloned, _from_objects
from sqlalchemy.sql.elements import _find_columns
from sqlalchemy.sql.selectable import Join, Lateral

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


def _find_left_clause_to_join_from(clauses, join_to, onclause):
    """Given a list of FROM clauses, a selectable,
    and optional ON clause, return a list of integer indexes from the
    clauses list indicating the clauses that can be joined from.

    The presence of an "onclause" indicates that at least one clause can
    definitely be joined from; if the list of clauses is of length one
    and the onclause is given, returns that index.   If the list of clauses
    is more than length one, and the onclause is given, attempts to locate
    which clauses contain the same columns.

    """
    idx = []
    selectables = set(_from_objects(join_to))

    # if we are given more than one target clause to join
    # from, use the onclause to provide a more specific answer.
    # otherwise, don't try to limit, after all, "ON TRUE" is a valid
    # on clause
    if len(clauses) > 1 and onclause is not None:
        resolve_ambiguity = True
        cols_in_onclause = _find_columns(onclause)
    else:
        resolve_ambiguity = False
        cols_in_onclause = None

    for i, f in enumerate(clauses):
        for s in selectables.difference([f]):
            if resolve_ambiguity:
                if set(f.c).union(s.c).issuperset(cols_in_onclause):
                    idx.append(i)
                    break
            elif onclause is not None or Join._can_join(f, s):
                idx.append(i)
                break
            elif onclause is None and isinstance(s, Lateral):
                # onclause is not accepted for lateral due to BCR change:
                # https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
                idx.append(i)
                break

    if len(idx) > 1:
        # this is the same "hide froms" logic from
        # Selectable._get_display_froms
        toremove = set(chain(*[_expand_cloned(f._hide_froms) for f in clauses]))
        idx = [i for i in idx if clauses[i] not in toremove]

    # onclause was given and none of them resolved, so assume
    # all indexes can match
    if not idx and onclause is not None:
        return range(len(clauses))
    else:
        return idx
