#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import base64
import json
import logging.handlers
import os
import time
import urllib.error
import urllib.request
import uuid
from logging import getLogger
from typing import Literal

import pytest
import snowflake.connector
import snowflake.connector.connection
import snowflake.connector.errors
from snowflake.connector.compat import IS_WINDOWS
from snowflake.connector.network import WORKLOAD_IDENTITY_AUTHENTICATOR
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import ProgrammingError as SAProgrammingError
from sqlalchemy.pool import NullPool

from snowflake.sqlalchemy import URL, dialect
from snowflake.sqlalchemy._constants import (
    APPLICATION_NAME,
    PARAM_APPLICATION,
    PARAM_INTERNAL_APPLICATION_NAME,
    PARAM_INTERNAL_APPLICATION_VERSION,
    SNOWFLAKE_SQLALCHEMY_VERSION,
)
from snowflake.sqlalchemy.snowdialect import _URL_QUERY_BLOCKED_KWARGS

try:
    from .parameters import CONNECTION_PARAMETERS
except ImportError:
    CONNECTION_PARAMETERS = None

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


def pytest_addoption(parser):
    parser.addoption(
        "--case-sensitive",
        action="store_true",
        default=False,
        help="run tests with case_sensitive_identifiers=True",
    )


logger = getLogger(__name__)

TZ_ENV_VAR: Literal["TZ"] = "TZ"
DEFAULT_TZ_VALUE: Literal["UTC"] = "UTC"

DEFAULT_PARAMETERS = {
    "account": "<account_name>",
    "user": "<user_name>",
    "database": "<database_name>",
    "schema": "<schema_name>",
    "protocol": "https",
    "host": "<host>",
    "port": "443",
}


# Single source of truth for Snowflake connection parameters, as
# ``(env var, connector param, forward_to_raw_connector)`` triples.  Both the
# environment-reading map and the raw-connector allowlist are derived from this
# list so the two can never drift apart.  ``forward`` is ``False`` only for
# ``schema``: ``get_db_parameters`` sets ``schema`` to the per-run ``TEST_SCHEMA``
# that does not exist yet, so it must not be sent to the raw connector (which
# would try to ``USE`` it) in the ``init_test_schema`` fixture.
_PARAM_SPEC: tuple[tuple[str, str, bool], ...] = (
    ("SNOWFLAKE_ACCOUNT", "account", True),
    ("SNOWFLAKE_USER", "user", True),
    ("SNOWFLAKE_PASSWORD", "password", True),
    ("SNOWFLAKE_DATABASE", "database", True),
    ("SNOWFLAKE_SCHEMA", "schema", False),
    ("SNOWFLAKE_HOST", "host", True),
    ("SNOWFLAKE_PORT", "port", True),
    ("SNOWFLAKE_PROTOCOL", "protocol", True),
    ("SNOWFLAKE_WAREHOUSE", "warehouse", True),
    ("SNOWFLAKE_ROLE", "role", True),
    ("SNOWFLAKE_AUTHENTICATOR", "authenticator", True),
    ("SNOWFLAKE_TOKEN", "token", True),
    ("SNOWFLAKE_WORKLOAD_IDENTITY_PROVIDER", "workload_identity_provider", True),
)
_ENV_MAP = {env: param for env, param, _ in _PARAM_SPEC}
_CONNECTOR_KEYS = frozenset(param for _, param, forward in _PARAM_SPEC if forward)


def _build_connector_kwargs(ret: dict) -> dict:
    return {k: ret[k] for k in _CONNECTOR_KEYS if ret.get(k)}


def _jwt_exp(token: str) -> float:
    """Best-effort read of a JWT's ``exp`` (epoch seconds); 0.0 if unparseable."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)  # restore base64 padding
        return float(json.loads(base64.urlsafe_b64decode(payload)).get("exp", 0.0))
    except (IndexError, ValueError, TypeError):
        # Malformed token (bad segments/base64/JSON or non-numeric exp): treat as
        # already expired so the caller mints a fresh one.  binascii.Error and
        # json.JSONDecodeError both subclass ValueError.
        return 0.0


class WifTokenProvider:
    """Mints and caches a fresh GitHub OIDC token for Snowflake WIF.

    GitHub Actions OIDC tokens are short-lived.  ``snowflake-actions`` mints one
    ``SNOWFLAKE_TOKEN`` at job start, but a full test run opens many new
    connections (NullPool) over 10+ minutes and outlives that token, causing
    "JWT is outside its validity period" failures partway through.  This provider
    re-mints a fresh token on demand from the GitHub endpoint (available for the
    whole job via ``ACTIONS_ID_TOKEN_REQUEST_URL``/``TOKEN``), re-minting once the
    cached token is within ``safety_seconds`` of its own ``exp`` claim so a cached
    token is never served expired.  Falls back to the static ``SNOWFLAKE_TOKEN``
    when the GitHub OIDC request endpoint is not available (e.g. local runs).
    """

    def __init__(self, safety_seconds: int = 60) -> None:
        self._token: str | None = None
        self._exp: float = 0.0
        self._safety = safety_seconds

    def __call__(self) -> str | None:
        req_url = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_URL")
        req_token = os.environ.get("ACTIONS_ID_TOKEN_REQUEST_TOKEN")
        if not req_url or not req_token:
            return os.environ.get("SNOWFLAKE_TOKEN")

        if self._token is not None and time.time() < self._exp - self._safety:
            return self._token

        token = self._mint(req_url, req_token)
        self._token = token
        self._exp = _jwt_exp(token) if token else 0.0
        return token

    @staticmethod
    def _mint(req_url: str, req_token: str) -> str | None:
        request = urllib.request.Request(
            f"{req_url}&audience=snowflakecomputing.com",
            headers={"Authorization": f"bearer {req_token}"},
        )
        # The GitHub OIDC token endpoint intermittently returns transient 5xx/429
        # responses.  A long test run re-mints many times (the do_connect listener
        # fires on every new connection), so retry with exponential backoff rather
        # than failing a single connection on a blip.
        for attempt in range(5):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    return json.load(response)["value"]
            except urllib.error.HTTPError as err:
                if err.code not in (429, 500, 502, 503, 504) or attempt == 4:
                    raise
            except urllib.error.URLError:
                if attempt == 4:
                    raise
            time.sleep(2**attempt)
        return None


# Single instance owns the token cache; both the env-parameter path and the
# per-connection do_connect listener re-mint through it, so the WIF token logic
# lives in exactly one place.
_wif_token = WifTokenProvider()


def _connection_parameters_from_env() -> dict:
    params = {
        param: os.environ[env] for env, param in _ENV_MAP.items() if os.environ.get(env)
    }
    # Under WIF, replace the (possibly stale) static token with a freshly minted
    # one so long test runs do not fail once the initial token expires.
    if params.get("authenticator") == WORKLOAD_IDENTITY_AUTHENTICATOR:
        fresh = _wif_token()
        if fresh:
            params["token"] = fresh
    return params


@event.listens_for(Engine, "do_connect")
def _inject_wif_token(dialect, conn_rec, cargs, cparams):
    """Inject a fresh OIDC token into every engine connection under WIF.

    Engines (notably the sqlalchemy compliance-suite engine) are created once
    but connect repeatedly over a long run; the token embedded in the URL at
    engine-creation time expires.  Refreshing here keeps every new connection
    authenticated regardless of how/when the engine was built.
    """
    if cparams.get("authenticator") == WORKLOAD_IDENTITY_AUTHENTICATOR:
        token = _wif_token()
        if token:
            cparams["token"] = token


@pytest.fixture(scope="session")
def db_parameters():
    yield get_db_parameters()


@pytest.fixture(scope="session")
def external_volume():
    db_parameters = get_db_parameters()
    if "external_volume" in db_parameters:
        yield db_parameters["external_volume"]
    else:
        raise ValueError("External_volume is not set")


@pytest.fixture(scope="session")
def external_stage():
    db_parameters = get_db_parameters()
    if "external_stage" in db_parameters:
        yield db_parameters["external_stage"]
    else:
        raise ValueError("External_stage is not set")


@pytest.fixture(scope="session")
def on_public_ci():
    return running_on_public_ci()


@pytest.fixture()
def default_warehouse(db_parameters, engine_testaccount):
    wh = db_parameters.get("warehouse")
    if wh is None:
        with engine_testaccount.connect() as conn:
            wh = conn.exec_driver_sql("SELECT CURRENT_WAREHOUSE()").scalar()
    if wh is None:
        pytest.fail("No warehouse configured for the current user/session")
    return wh


@pytest.fixture(scope="function")
def base_location(external_stage, engine_testaccount):
    unique_id = str(uuid.uuid4())
    base_location = "L" + unique_id.replace("-", "_")
    yield base_location
    remove_base_location = f"""
    REMOVE @{external_stage} pattern='.*{base_location}.*';
     """
    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(remove_base_location)


# Snowflake error numbers for features not available on all account tiers.
_FEATURE_UNAVAILABLE_ERRNOS = {
    391404,  # Hybrid tables not available to trial accounts
}


def _feature_unavailable_reason(exc: BaseException) -> str | None:
    """Return a skip reason if exc signals a Snowflake feature not available, else None."""
    if isinstance(exc, SAProgrammingError) and exc.orig is not None:
        exc = exc.orig
    if (
        isinstance(exc, snowflake.connector.errors.ProgrammingError)
        and exc.errno in _FEATURE_UNAVAILABLE_ERRNOS
    ):
        return f"feature not available on this account: {exc}"
    return None


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed and call.excinfo is not None:
        reason = _feature_unavailable_reason(call.excinfo.value)
        if reason:
            report.outcome = "skipped"
            report.longrepr = (item.location[0], item.location[1], f"Skipped: {reason}")


def get_db_parameters() -> dict:
    """
    Sets the db connection parameters
    """
    ret = {}
    os.environ[TZ_ENV_VAR] = DEFAULT_TZ_VALUE
    if hasattr(time, "tzset"):
        if not IS_WINDOWS:
            time.tzset()
    else:
        logger.warning("time.tzset is unavailable on this platform")

    ret.update(DEFAULT_PARAMETERS)
    if CONNECTION_PARAMETERS is not None:
        ret.update(CONNECTION_PARAMETERS)
    else:
        ret.update(_connection_parameters_from_env())

    assert ret["account"] != DEFAULT_PARAMETERS["account"], "account not configured"

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


def _without_blocked_query_params(url):
    """Return *url* with connector-only parameters removed from the query string.

    The dialect accepts these parameters (e.g. ``protocol``) only via
    ``connect_args=`` in ``create_engine``, not via the URL query string.  The
    test connection parameters historically carry them in the URL; this helper
    keeps the integration tests on the default, strict code path by dropping
    them from the query.  Values such as ``protocol=https`` simply fall back to
    the connector defaults, and ``host``/``port`` are unaffected because they
    live in the URL authority rather than the query.

    Tests that specifically exercise the legacy URL behaviour enable
    ``SNOWFLAKE_SQLALCHEMY_LEGACY_URL_PARAMS`` themselves and do not use this
    helper.
    """
    url = make_url(url)
    query = {k: v for k, v in url.query.items() if k not in _URL_QUERY_BLOCKED_KWARGS}
    return url.set(query=query)


def url_factory(**kwargs):
    # ``password`` is only present when a password is configured (there is no
    # ``None`` sentinel under WIF), so ``URL()`` never receives a NoneType to
    # rfc-1738-quote.
    url_params = get_db_parameters()
    url_params.update(kwargs)
    return _without_blocked_query_params(URL(**url_params))


def get_engine(url, **engine_kwargs):
    engine_params = {
        "poolclass": NullPool,
        "future": True,
        "echo": True,
    }
    engine_params.update(engine_kwargs)

    connect_args = engine_params.get("connect_args", {}).copy()
    connect_args["disable_ocsp_checks"] = True
    connect_args["insecure_mode"] = True
    engine_params["connect_args"] = connect_args

    engine = create_engine(_without_blocked_query_params(url), **engine_params)
    return engine


@pytest.fixture()
def engine_testaccount(request):
    url = url_factory()
    cs = request.config.getoption("--case-sensitive")
    engine = get_engine(url, case_sensitive_identifiers=cs)
    request.addfinalizer(engine.dispose)
    yield engine


@pytest.fixture()
def engine_testaccount_case_sensitive(request):
    url = url_factory()
    engine = get_engine(url, case_sensitive_identifiers=True)
    request.addfinalizer(engine.dispose)
    yield engine


@pytest.fixture()
def assert_text_in_buf():
    buf = logging.handlers.BufferingHandler(100)
    for log in [
        logging.getLogger("sqlalchemy.engine"),
    ]:
        log.addHandler(buf)

    def go(expected, occurrences=1):
        assert buf.buffer
        buflines = [rec.getMessage() for rec in buf.buffer]
        ocurrences_found = 0
        for line in buflines:
            if line.find(expected) != -1:
                ocurrences_found += 1

        assert occurrences == ocurrences_found, (
            f"Expected {occurrences} of {expected}, got {ocurrences_found} "
            f"occurrences in {buflines}."
        )
        buf.flush()

    yield go
    for log in [
        logging.getLogger("sqlalchemy.engine"),
    ]:
        log.removeHandler(buf)


@pytest.fixture()
def engine_testaccount_with_numpy(request):
    url = url_factory(numpy=True)
    cs = request.config.getoption("--case-sensitive")
    engine = get_engine(url, case_sensitive_identifiers=cs)
    request.addfinalizer(engine.dispose)
    yield engine


@pytest.fixture()
def engine_testaccount_with_qmark(request):
    snowflake.connector.paramstyle = "qmark"

    url = url_factory()
    cs = request.config.getoption("--case-sensitive")
    engine = get_engine(url, case_sensitive_identifiers=cs)
    request.addfinalizer(engine.dispose)

    yield engine

    snowflake.connector.paramstyle = "pyformat"


@pytest.fixture(scope="session", autouse=True)
def init_test_schema(request, db_parameters):
    # Fetch parameters fresh (rather than reusing the session-scoped
    # ``db_parameters``) so the raw-connector connection uses a currently-valid
    # WIF token.  The teardown runs at the end of a long session, well after the
    # token captured at session start would have expired.
    with snowflake.connector.connect(
        **_build_connector_kwargs(get_db_parameters())
    ) as con:
        con.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}")

    def fin():
        with snowflake.connector.connect(
            **_build_connector_kwargs(get_db_parameters())
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


def running_on_public_ci() -> bool:
    """Whether or not tests are currently running on one of our public CIs."""
    return os.getenv("GITHUB_ACTIONS") == "true"


def pytest_runtest_setup(item) -> None:
    """Ran before calling each test, used to decide whether a test should be skipped."""
    _ensure_optional_dependencies(item)
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


def _ensure_optional_dependencies(item):
    """Skip optional-dependency tests when the dependency is unavailable."""
    if "pandas" in item.keywords:
        pytest.importorskip("pandas")


def poll_until(fn, *, timeout: float = 15, interval: float = 1):
    """Poll ``fn()`` every *interval* seconds until it returns a truthy value.

    Returns the first truthy result, or the last (falsy) result once *timeout*
    is exceeded.

    Use this instead of ``pytest.mark.flaky(reruns=N)`` when the test creates
    Snowflake objects (tables, indexes, etc.) that should not be torn down and
    recreated on every retry attempt.  Re-running the whole test via
    ``pytest-rerunfailures`` would execute the fixture teardown and re-run
    setup DDL on each attempt, which is wasteful and can hit object-already-
    exists errors.  Polling within the test body avoids touching the data.
    """
    deadline = time.monotonic() + timeout
    while True:
        result = fn()
        if result or time.monotonic() > deadline:
            return result
        time.sleep(interval)
