#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy.dialects import registry

registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
registry.register("snowflake.snowflake", "snowflake.sqlalchemy", "dialect")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa
from sqlalchemy.testing.plugin.pytestplugin import (
    pytest_sessionfinish as _pytest_sessionfinish,
)
from sqlalchemy.testing.plugin.pytestplugin import (
    pytest_sessionstart as _pytest_sessionstart,
)

import snowflake.connector
from snowflake.sqlalchemy import URL

from ..conftest import get_db_parameters


def pytest_sessionstart(session):
    db_parameters = get_db_parameters()
    session.config.option.dburi = [URL(**db_parameters)]
    # schema name with 'TEST_SCHEMA' is required by some tests of the sqlalchemy test suite
    with snowflake.connector.connect(**db_parameters) as con:
        con.cursor().execute("CREATE SCHEMA IF NOT EXISTS TEST_SCHEMA")
    _pytest_sessionstart(session)


def pytest_sessionfinish(session):
    _pytest_sessionfinish(session)
