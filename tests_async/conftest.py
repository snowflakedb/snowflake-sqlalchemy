#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Bootstrap sqlalchemy.testing for the rebased dialect-specific tests.

Mirrors tests/sqlalchemy_test_suite/conftest.py: the connection URL is built
from get_db_parameters() (which reads CI env vars / tests/parameters.py), so
these tests run on the same credentials as the rest of the suite.

Set SNOWFLAKE_ASYNC_TESTS=1 to run against the async driver
(snowflake+snowflake_async://). The sqlalchemy.testing framework runs each test in
a greenlet when config.is_async, so the same TablesTest/TestBase classes cover
both sync and async without changes.
"""

import os

import snowflake.connector
from sqlalchemy.dialects import registry
from sqlalchemy.testing.plugin.pytestplugin import *  # noqa: F401,F403
from sqlalchemy.testing.plugin.pytestplugin import (
    pytest_sessionfinish as _pytest_sessionfinish,
)
from sqlalchemy.testing.plugin.pytestplugin import (
    pytest_sessionstart as _pytest_sessionstart,
)

from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy.snowdialect import _KEEPALIVE_AUTODETECT_SUPPORTED, dialect
from tests.conftest import _without_blocked_query_params, get_db_parameters

_identifier_preparer = dialect().identifier_preparer


def _setup_connect_kwargs():
    """Kwargs for the sync setup/teardown connections. On connector 5.x, pin
    enable_server_session_keep_alive_auto_detection to silence the connector's
    FutureWarning (matching what the dialect does for engine connections);
    no-op on connector 4.x, which lacks the parameter."""
    kwargs = get_db_parameters()
    if _KEEPALIVE_AUTODETECT_SUPPORTED:
        kwargs.setdefault("enable_server_session_keep_alive_auto_detection", True)
    return kwargs


# All three entrypoints must be registered: the sqlalchemy.testing provisioning
# layer rebuilds the URL as "<backend>+<default_driver>" (i.e.
# "snowflake+snowflake") and silently yields None if that name is unregistered.
registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
registry.register("snowflake.snowflake", "snowflake.sqlalchemy", "dialect")
registry.register(
    "snowflake.snowflake_async",
    "snowflake.sqlalchemy._async.async_dialect",
    "SnowflakeDialect_async",
)


def pytest_sessionstart(session):
    db_parameters = get_db_parameters()
    url = _without_blocked_query_params(URL(**db_parameters))
    if os.environ.get("SNOWFLAKE_ASYNC_TESTS") == "1":
        url = url.set(drivername="snowflake+snowflake_async")
    # Pass the URL object itself rather than a rendered string: sqlalchemy.testing
    # calls sqlalchemy.engine.make_url() on each dburi entry, which returns a URL
    # instance unchanged (no re-stringification), so the real password stays
    # available for the actual connection without ever materializing as an
    # unmasked plaintext string. str(url)/repr(url) (e.g. incidental logging)
    # continue to mask the password by default.
    session.config.option.dburi = [url]
    # get_db_parameters() generates a per-run schema for isolation; create it
    # up front (setup/teardown always use the synchronous connector).
    with snowflake.connector.connect(**_setup_connect_kwargs()) as con:
        con.cursor().execute(
            f"CREATE SCHEMA IF NOT EXISTS "
            f"{_identifier_preparer.quote(db_parameters['schema'])}"
        )
    _pytest_sessionstart(session)


def pytest_sessionfinish(session):
    db_parameters = get_db_parameters()
    with snowflake.connector.connect(**_setup_connect_kwargs()) as con:
        con.cursor().execute(
            f"DROP SCHEMA IF EXISTS "
            f"{_identifier_preparer.quote(db_parameters['schema'])}"
        )
    _pytest_sessionfinish(session)
