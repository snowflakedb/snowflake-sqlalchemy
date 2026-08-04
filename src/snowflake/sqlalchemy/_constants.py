#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from snowflake.connector.errorcode import ER_CONNECTION_IS_CLOSED

from .version import VERSION

# parameters needed for usage tracking
PARAM_APPLICATION = "application"
PARAM_INTERNAL_APPLICATION_NAME = "internal_application_name"
PARAM_INTERNAL_APPLICATION_VERSION = "internal_application_version"

APPLICATION_NAME = "SnowflakeSQLAlchemy"
SNOWFLAKE_SQLALCHEMY_VERSION = VERSION
DIALECT_NAME = "snowflake"
NOT_NULL = "NOT NULL"

# Error codes that mean the session/token is gone (or the socket is already
# closed) and the connection must be treated as disconnected so the pool can
# recycle it (SNOW-669163). The 390xxx values are Snowflake server (GS) codes;
# the connector only exposes them as strings via ``snowflake.connector.network``
# while DBAPI errors report ``errno`` as an int, so they are listed here as
# ints. This set is the single source of truth and is imported by the tests.
DISCONNECT_ERROR_CODES = frozenset(
    {
        390110,  # ID token expired
        390111,  # session no longer exists
        390112,  # session expired
        390113,  # master token not found
        390114,  # authentication/master token expired
        390195,  # authentication token expired (variant)
        390115,  # master token invalid
        390318,  # OAuth access token expired
        ER_CONNECTION_IS_CLOSED,  # 250002, connection is closed (client side)
    }
)

# Set this environment variable to opt into the legacy behaviour where
# certain connection parameters are accepted as URL query-string
# values.  Applications relying on this should migrate to connect_args= in
# create_engine() instead.  Interpreted with parse_url_boolean — accepts "1" or
# "true" (case-insensitive); any other value leaves the shim disabled.
SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS = "SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS"
