#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from .version import VERSION

# parameters needed for usage tracking
PARAM_APPLICATION = "application"
PARAM_INTERNAL_APPLICATION_NAME = "internal_application_name"
PARAM_INTERNAL_APPLICATION_VERSION = "internal_application_version"

APPLICATION_NAME = "SnowflakeSQLAlchemy"
SNOWFLAKE_SQLALCHEMY_VERSION = VERSION
DIALECT_NAME = "snowflake"
NOT_NULL = "NOT NULL"

# Set this environment variable to opt into the legacy behaviour where
# certain connection parameters are accepted as URL query-string
# values.  Applications relying on this should migrate to connect_args= in
# create_engine() instead.  Interpreted with parse_url_boolean — accepts "1" or
# "true" (case-insensitive); any other value leaves the shim disabled.
SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS = "SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS"
