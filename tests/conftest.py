#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import os
import sys
import time
import uuid
from functools import partial
from logging import getLogger

import pytest
from sqlalchemy import create_engine

import snowflake.connector
import snowflake.connector.connection
from snowflake.connector.compat import IS_WINDOWS
from snowflake.sqlalchemy import URL, dialect
from snowflake.sqlalchemy._constants import (
    APPLICATION_NAME,
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)

from .parameters import CONNECTION_PARAMETERS

CLOUD_PROVIDERS = {"aws", "azure", "gcp"}
EXTERNAL_SKIP_TAGS = {"internal"}
INTERNAL_SKIP_TAGS = {"external"}
RUNNING_ON_GH = os.getenv("GITHUB_ACTIONS") == "true"

snowflake.connector.connection.DEFAULT_CONFIGURATION[PARAM_APPLICATION] = (
    APPLICATION_NAME,
    (type(None), str),
)
snowflake.connector.connection.DEFAULT_CONFIGURATION[
    PARAM_INTERNAL_APPLICATION_NAME
] = (APPLICATION_NAME, (type(None), str))
snowflake.connector.connection.DEFAULT_CONFIGURATION[
    PARAM_INTERNAL_APPLICATION_VERSION
] = (SNOWFLAKE_SQLALCHEMY_VERSION, (type(None), str))

TEST_SCHEMA = f"sqlalchemy_tests_{str(uuid.uuid4()).replace('-', '_')}"

create_engine_with_future_flag = create_engine


def pytest_addoption(parser):
    parser.addoption(
        "--run_v20_sqlalchemy",
        help="Use only 2.0 SQLAlchemy APIs, any legacy features (< 2.0) will not be supported."
        "Turning on this option will set future flag to True on Engine and Session objects according to"
        "the migration guide: https://docs.sqlalchemy.org/en/14/changelog/migration_20.html",
        action="store_true",
    )


@pytest.fixture(scope="session")
def on_travis():
    return os.getenv("TRAVIS", "").lower() == "true"


@pytest.fixture(scope="session")
def on_appveyor():
    return os.getenv("APPVEYOR", "").lower() == "true"


@pytest.fixture(scope="session")
def on_public_ci(on_travis, on_appveyor):
    return on_travis or on_appveyor


def help():
    print(
        """Connection parameter must be specified in parameters.py,
    for example:
CONNECTION_PARAMETERS = {
    'account': 'testaccount',
    'user': 'user1',
    'password': 'test',
    'database': 'testdb',
    'schema': 'public',
}"""
    )


logger = getLogger(__name__)

DEFAULT_PARAMETERS = {
    "account": "<account_name>",
    "user": "<user_name>",
    "password": "<password>",
    "database": "<database_name>",
    "schema": "<schema_name>",
    "protocol": "https",
    "host": "<host>",
    "port": "443",
}


@pytest.fixture(scope="session")
def db_parameters():
    return get_db_parameters()


def get_db_parameters():
    """
    Sets the db connection parameters
    """
    ret = {}
    os.environ["TZ"] = "UTC"
    if not IS_WINDOWS:
        time.tzset()
    for k, v in CONNECTION_PARAMETERS.items():
        ret[k] = v

    for k, v in DEFAULT_PARAMETERS.items():
        if k not in ret:
            ret[k] = v

    if "account" in ret and ret["account"] == DEFAULT_PARAMETERS["account"]:
        help()
        sys.exit(2)

    if "host" in ret and ret["host"] == DEFAULT_PARAMETERS["host"]:
        ret["host"] = ret["account"] + ".snowflakecomputing.com"

    # a unique table name
    ret["name"] = ("sqlalchemy_tests_" + str(uuid.uuid4())).replace("-", "_")
    ret["schema"] = TEST_SCHEMA

    # This reduces a chance to exposing password in test output.
    ret["a00"] = "dummy parameter"
    ret["a01"] = "dummy parameter"
    ret["a02"] = "dummy parameter"
    ret["a03"] = "dummy parameter"
    ret["a04"] = "dummy parameter"
    ret["a05"] = "dummy parameter"
    ret["a06"] = "dummy parameter"
    ret["a07"] = "dummy parameter"
    ret["a08"] = "dummy parameter"
    ret["a09"] = "dummy parameter"
    ret["a10"] = "dummy parameter"
    ret["a11"] = "dummy parameter"
    ret["a12"] = "dummy parameter"
    ret["a13"] = "dummy parameter"
    ret["a14"] = "dummy parameter"
    ret["a15"] = "dummy parameter"
    ret["a16"] = "dummy parameter"

    return ret


def get_engine(user=None, password=None, account=None, schema=None):
    """
    Creates a connection using the parameters defined in JDBC connect string
    """
    ret = get_db_parameters()

    if user is not None:
        ret["user"] = user
    if password is not None:
        ret["password"] = password
    if account is not None:
        ret["account"] = account

    from sqlalchemy.pool import NullPool

    engine = create_engine_with_future_flag(
        URL(
            user=ret["user"],
            password=ret["password"],
            host=ret["host"],
            port=ret["port"],
            database=ret["database"],
            schema=TEST_SCHEMA if not schema else schema,
            account=ret["account"],
            protocol=ret["protocol"],
        ),
        poolclass=NullPool,
    )

    return engine, ret


@pytest.fixture()
def engine_testaccount(request):
    engine, _ = get_engine()
    request.addfinalizer(engine.dispose)
    return engine


@pytest.fixture(scope="session", autouse=True)
def init_test_schema(request, db_parameters):
    ret = db_parameters
    with snowflake.connector.connect(
        user=ret["user"],
        password=ret["password"],
        host=ret["host"],
        port=ret["port"],
        database=ret["database"],
        account=ret["account"],
        protocol=ret["protocol"],
    ) as con:
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

    def fin():
        ret1 = db_parameters
        with snowflake.connector.connect(
            user=ret1["user"],
            password=ret1["password"],
            host=ret1["host"],
            port=ret1["port"],
            database=ret1["database"],
            account=ret1["account"],
            protocol=ret1["protocol"],
        ) as con1:
            con1.cursor().execute(f"DROP SCHEMA IF EXISTS {TEST_SCHEMA}")

    request.addfinalizer(fin)


@pytest.fixture(scope="session")
def sql_compiler():
    return lambda sql_command: str(
        sql_command.compile(
            dialect=dialect(),
            compile_kwargs={"literal_binds": True, "deterministic": True},
        )
    ).replace("\n", "")


@pytest.fixture(scope="session")
def run_v20_sqlalchemy(pytestconfig):
    return pytestconfig.option.run_v20_sqlalchemy


def pytest_sessionstart(session):
    # patch the create_engine with future flag
    global create_engine_with_future_flag
    create_engine_with_future_flag = partial(
        create_engine, future=session.config.option.run_v20_sqlalchemy
    )


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return os.getenv("GITHUB_ACTIONS") == "true"


def pytest_runtest_setup(item) -> None:
    """Ran before calling each test, used to decide whether a test should be skipped."""
    test_tags = [mark.name for mark in item.iter_markers()]

    # Get what cloud providers the test is marked for if any
    test_supported_providers = CLOUD_PROVIDERS.intersection(test_tags)
    # Default value means that we are probably running on a developer's machine, allow everything in this case
    current_provider = os.getenv("cloud_provider", "dev")
    if test_supported_providers:
        # If test is tagged for specific cloud providers add the default cloud_provider as supported too
        test_supported_providers.add("dev")
        if current_provider not in test_supported_providers:
            pytest.skip(
                f"cannot run unit test against cloud provider {current_provider}"
            )
    if EXTERNAL_SKIP_TAGS.intersection(test_tags) and running_on_public_ci():
        pytest.skip("cannot run this test on external CI")
    elif INTERNAL_SKIP_TAGS.intersection(test_tags) and not running_on_public_ci():
        pytest.skip("cannot run this test on internal CI")
