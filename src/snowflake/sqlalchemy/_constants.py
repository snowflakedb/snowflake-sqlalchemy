#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pkg_resources

# parameters needed for usage tracking
PARAM_APPLICATION = "application"
PARAM_INTERNAL_APPLICATION_NAME = "internal_application_name"
PARAM_INTERNAL_APPLICATION_VERSION = "internal_application_version"

APPLICATION_NAME = "SnowflakeSQLAlchemy"
SNOWFLAKE_SQLALCHEMY_VERSION = pkg_resources.get_distribution(
    "snowflake-sqlalchemy"
).version
