#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from urllib.parse import quote_plus

from sqlalchemy import exc
from sqlalchemy.engine.url import _rfc_1738_quote

from snowflake.connector.compat import IS_STR


def _url(**db_parameters):
    """
    Composes a SQLAlchemy connect string from the given database connection
    parameters.
    """
    specified_parameters = []
    if "account" not in db_parameters:
        raise exc.ArgumentError("account parameter must be specified.")

    if "host" in db_parameters:
        ret = f"snowflake://{db_parameters.get('user', '')}:{_rfc_1738_quote(db_parameters.get('password', ''))}@{db_parameters['host']}:{db_parameters['port'] if 'port' in db_parameters else 443}/"
        specified_parameters += ["user", "password", "host", "port"]
    elif "region" not in db_parameters:
        ret = f"snowflake://{db_parameters.get('user', '')}:{_rfc_1738_quote(db_parameters.get('password', ''))}@{db_parameters['account']}/"
        specified_parameters += ["user", "password", "account"]
    else:
        ret = f"snowflake://{db_parameters.get('user', '')}:{_rfc_1738_quote(db_parameters.get('password', ''))}@{db_parameters['account']}.{db_parameters['region']}/"
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


def _sort_columns_by_sequences(columns_sequences, columns):
    return [col for _, col in sorted(zip(columns_sequences, columns))]
