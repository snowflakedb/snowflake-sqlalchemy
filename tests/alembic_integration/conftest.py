#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Stands in for a user's ``alembic/env.py``: importing the implementation is
# what registers it for the ``snowflake`` dialect. Loading the dialect alone
# does not.
from snowflake.sqlalchemy.alembic_impl import SnowflakeImpl  # noqa: F401
