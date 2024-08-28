# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlalchemy.testing.config
from sqlalchemy import util
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
from snowflake.sqlalchemy.compat import IS_VERSION_20

from ..conftest import get_db_parameters
from ..util import random_string

registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
registry.register("snowflake.snowflake", "snowflake.sqlalchemy", "dialect")
TEST_SCHEMA = f"test_schema_{random_string(5)}"
TEST_SCHEMA_2 = f"{TEST_SCHEMA}_2"


if IS_VERSION_20:
    collect_ignore_glob = ["test_suite.py"]
else:
    collect_ignore_glob = ["test_suite_20.py"]


# patch sqlalchemy.testing.config.Confi.__init__ for schema name randomization
# same schema name would result in conflict as we're running tests in parallel in the CI
def config_patched__init__(self, db, db_opts, options, file_config):
    self._set_name(db)
    self.db = db
    self.db_opts = db_opts
    self.options = options
    self.file_config = file_config
    self.test_schema = TEST_SCHEMA
    self.test_schema_2 = TEST_SCHEMA_2

    self.is_async = db.dialect.is_async and not util.asbool(
        db.url.query.get("async_fallback", False)
    )


sqlalchemy.testing.config.Config.__init__ = config_patched__init__


def pytest_sessionstart(session):
    db_parameters = get_db_parameters()
    session.config.option.dburi = [URL(**db_parameters)]
    # schema name with 'TEST_SCHEMA' is required by some tests of the sqlalchemy test suite
    with snowflake.connector.connect(**db_parameters) as con:
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {db_parameters['schema']}")
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA};")
    _pytest_sessionstart(session)


def pytest_sessionfinish(session):
    db_parameters = get_db_parameters()
    with snowflake.connector.connect(**db_parameters) as con:
        con.cursor().execute(f"DROP SCHEMA IF EXISTS {db_parameters['schema']}")
        con.cursor().execute(f"DROP SCHEMA IF EXISTS f{TEST_SCHEMA}")
    _pytest_sessionfinish(session)
