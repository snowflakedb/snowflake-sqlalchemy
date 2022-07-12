#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy.dialects import registry
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

registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
registry.register("snowflake.snowflake", "snowflake.sqlalchemy", "dialect")
sqlalchemy_test_schema = "TEST_SCHEMA"


def pytest_sessionstart(session):
    db_parameters = get_db_parameters()
    session.config.option.dburi = [URL(**db_parameters)]
    # schema name with 'TEST_SCHEMA' is required by some tests of the sqlalchemy test suite
    with snowflake.connector.connect(**db_parameters) as con:
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {db_parameters['schema']}")
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {sqlalchemy_test_schema};")
    _pytest_sessionstart(session)


def pytest_sessionfinish(session):
    db_parameters = get_db_parameters()
    with snowflake.connector.connect(**db_parameters) as con:
        con.cursor().execute(f"DROP SCHEMA IF EXISTS {db_parameters['schema']}")
        con.cursor().execute(f"DROP SCHEMA IF EXISTS f{sqlalchemy_test_schema}")
    _pytest_sessionfinish(session)
