#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from alembic.ddl.impl import DefaultImpl


class SnowflakeImpl(DefaultImpl):
    """Register the Snowflake dialect with Alembic's migration runtime.

    Without this, direct ``MigrationContext.configure(...)`` calls on a
    Snowflake connection fail with ``KeyError: "snowflake"``.
    """

    __dialect__ = "snowflake"
