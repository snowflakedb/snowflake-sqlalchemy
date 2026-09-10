#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import warnings
from contextlib import contextmanager
from sys import modules
from types import SimpleNamespace
from unittest import mock

import pytest
import sqlalchemy
from sqlalchemy import __version__ as SQLALCHEMY_VERSION
from sqlalchemy.engine import default as sqla_default
from sqlalchemy.engine.url import URL as SAUrl

from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy._constants import DISCONNECT_ERROR_CODES
from snowflake.sqlalchemy.snowdialect import (
    SnowflakeDialect,
    TelemetryEvents,
)

#: Default flag values emitted by ``_log_new_connection_event`` when the
#: dialect is constructed without overrides.  Kept here so tests can extend
#: this dict incrementally rather than repeating the full payload shape.
_DEFAULT_FLAG_PAYLOAD = {
    "case_sensitive_identifiers": False,
    "enable_decfloat": False,
    "enable_structured_type_json": True,
}


@pytest.fixture
def fake_connection():
    return SimpleNamespace(
        host="example.snowflakecomputing.com",
        port=443,
        application="test_app",
    )


class _RecordingAdapter:
    """Connector-version-agnostic telemetry double.

    ``get_adapter`` is the dialect's only version-specific seam, so patching it
    lets these tests assert the *gather* contract -- which events are emitted, in
    what order, with what payload, and the PII rules -- without depending on
    which connector's send API happens to be installed.  The per-version send
    mechanics (4.x ``TelemetryClient``/``send_batch`` vs 5.x
    ``try_add_log_to_batch``/no-op flush) are covered in
    ``tests/test_telemetry_adapter.py``.
    """

    def __init__(self, fail_on_register: bool = False):
        self.registered: list[tuple[str, object]] = []
        self.flushed = 0
        self._fail_on_register = fail_on_register

    def register(self, event_type, value, *, connection):
        if self._fail_on_register:
            raise RuntimeError("boom")
        self.registered.append((event_type, value))

    def flush(self, *, connection):
        self.flushed += 1

    def is_enabled(self, *, connection):
        return True


@contextmanager
def _recording_adapter(**kwargs):
    """Patch the dialect's telemetry dispatch to yield a recording adapter."""
    adapter = _RecordingAdapter(**kwargs)
    with mock.patch(
        "snowflake.sqlalchemy._telemetry.get_adapter", return_value=adapter
    ):
        yield adapter


def _legacy_value(adapter):
    """Return the ``value`` of the legacy NEW_CONNECTION event (registered first)."""
    event_type, value = adapter.registered[0]
    assert event_type == TelemetryEvents.NEW_CONNECTION.value
    return value


def _structured_value(adapter):
    """Return the ``value`` of the structured event (registered second)."""
    event_type, value = adapter.registered[1]
    assert event_type == TelemetryEvents.NEW_CONNECTION_PARAMETERS.value
    return value


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_connect_sends_telemetry(mock_connect, fake_connection):
    """Ensure telemetry is registered with the expected payload on connect."""
    mock_connect.return_value = fake_connection

    # Mock out pandas to ensure deterministic behavior
    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        result = SnowflakeDialect().connect()

    assert result is fake_connection
    assert _legacy_value(adapter) == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **_DEFAULT_FLAG_PAYLOAD}
    )
    # Both events are registered, then a single flush is requested.
    assert adapter.flushed == 1


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_connect_telemetry_includes_pandas_when_available(
    mock_connect, fake_connection
):
    """Ensure telemetry includes pandas version when pandas is installed."""
    mock_connect.return_value = fake_connection

    # Create a mock pandas module with a version
    mock_pandas = mock.MagicMock()
    mock_pandas.__version__ = "2.1.0"

    with (
        mock.patch.dict(modules, {"pandas": mock_pandas}),
        _recording_adapter() as adapter,
    ):
        SnowflakeDialect().connect()

    assert _legacy_value(adapter) == str(
        {
            "SQLAlchemy": SQLALCHEMY_VERSION,
            "pandas": "2.1.0",
            **_DEFAULT_FLAG_PAYLOAD,
        }
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_connect_telemetry_excludes_pandas_when_not_available(
    mock_connect, fake_connection
):
    """Ensure telemetry does not include pandas when it is not installed."""
    mock_connect.return_value = fake_connection

    # Simulate pandas not being installed
    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        SnowflakeDialect().connect()

    assert _legacy_value(adapter) == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **_DEFAULT_FLAG_PAYLOAD}
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_connect_logs_when_telemetry_fails(mock_connect, caplog, fake_connection):
    """Ensure failures in telemetry do not break connect and are logged.

    The failure is injected at the adapter seam (``register`` raises) rather than
    by breaking a specific connector's telemetry client, so this asserts the
    dialect's swallow-and-log contract on any connector version.
    """
    mock_connect.return_value = fake_connection

    caplog.set_level("DEBUG", logger="snowflake.sqlalchemy.snowdialect")

    with _recording_adapter(fail_on_register=True) as adapter:
        result = SnowflakeDialect().connect()

    # connect() still succeeds and nothing was recorded (register raised).
    assert result is fake_connection
    assert adapter.registered == []
    assert adapter.flushed == 0
    assert any(
        "Failed to send telemetry data" in message for message in caplog.messages
    )


# ---------------------------------------------------------------------------
# Dialect flags are recorded in the NEW_CONNECTION telemetry payload
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ctor_kwargs,expected_overrides",
    [
        (
            {"case_sensitive_identifiers": True},
            {"case_sensitive_identifiers": True},
        ),
        (
            {"enable_decfloat": True},
            {"enable_decfloat": True},
        ),
        (
            {"enable_structured_type_json": False},
            {"enable_structured_type_json": False},
        ),
        (
            # All flags flipped away from their defaults at once
            {
                "case_sensitive_identifiers": True,
                "enable_decfloat": True,
                "enable_structured_type_json": False,
            },
            {
                "case_sensitive_identifiers": True,
                "enable_decfloat": True,
                "enable_structured_type_json": False,
            },
        ),
    ],
    ids=[
        "case_sensitive_identifiers_true",
        "enable_decfloat_true",
        "enable_structured_type_json_false",
        "all_flipped",
    ],
)
@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_connect_telemetry_records_kwarg_flags(
    mock_connect,
    fake_connection,
    ctor_kwargs,
    expected_overrides,
):
    """Constructor-kwarg flags reach the telemetry payload unchanged."""
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        # Some overrides intentionally set now-deprecated legacy values
        # (enable_structured_type_json=False), which emit a DeprecationWarning
        # at construction; suppress it here since this test asserts telemetry
        # propagation, not the warnings.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            dialect = SnowflakeDialect(**ctor_kwargs)
        dialect.connect()

    expected_flags = {**_DEFAULT_FLAG_PAYLOAD, **expected_overrides}
    assert _legacy_value(adapter) == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **expected_flags}
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_connect_telemetry_records_url_driven_flag(mock_connect, fake_connection):
    """A URL-driven flag is reflected on the first new-connection event.

    ``create_connect_args`` runs before ``connect()`` returns, and the
    telemetry event is emitted *after* the connection is established.  The
    flag value captured on the event must therefore be the post-URL value,
    not the constructor default.
    """
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        dialect = SnowflakeDialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query={"case_sensitive_identifiers": "True"},
        )
        # Trigger the URL-parameter application prior to connect().
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            dialect.create_connect_args(url)

        dialect.connect()

    expected_flags = {**_DEFAULT_FLAG_PAYLOAD, "case_sensitive_identifiers": True}
    assert _legacy_value(adapter) == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **expected_flags}
    )


# ---------------------------------------------------------------------------
# json_serializer / json_deserializer engine parameters (SNOW-889293)
# ---------------------------------------------------------------------------


def test_dialect_accepts_json_serializer_and_deserializer_kwargs():
    """Constructor accepts and stores json_serializer/json_deserializer."""

    def serializer(obj):
        return obj

    def deserializer(value):
        return value

    dialect = SnowflakeDialect(
        json_serializer=serializer, json_deserializer=deserializer
    )

    assert dialect._json_serializer is serializer
    assert dialect._json_deserializer is deserializer


def test_dialect_json_serializers_default_to_none():
    """When not provided the json (de)serializers default to None."""
    dialect = SnowflakeDialect()

    assert dialect._json_serializer is None
    assert dialect._json_deserializer is None


def test_create_engine_routes_json_serializer_and_deserializer():
    """create_engine no longer raises when json (de)serializers are passed."""

    def serializer(obj):
        return obj

    def deserializer(value):
        return value

    engine = sqlalchemy.create_engine(
        URL(account="testaccount", user="u", password="p"),
        json_serializer=serializer,
        json_deserializer=deserializer,
    )

    assert engine.dialect._json_serializer is serializer
    assert engine.dialect._json_deserializer is deserializer


# ---------------------------------------------------------------------------
# is_disconnect / connection_invalidated detection (SNOW-669163)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("errno", sorted(DISCONNECT_ERROR_CODES))
def test_is_disconnect_true_for_session_and_token_errors(errno):
    """Session/token loss must be reported as a disconnect."""
    from snowflake.connector.errors import ProgrammingError

    dialect = SnowflakeDialect()
    error = ProgrammingError(msg="boom", errno=errno)

    assert dialect.is_disconnect(error, None, None) is True


def test_is_disconnect_false_for_regular_sql_error():
    """A plain SQL error (e.g. compilation) is not a disconnect."""
    from snowflake.connector.errors import ProgrammingError

    dialect = SnowflakeDialect()
    error = ProgrammingError(msg="SQL compilation error", errno=1003)

    assert dialect.is_disconnect(error, None, None) is False


def test_is_disconnect_false_for_non_snowflake_error():
    """Non-connector exceptions with a matching errno are ignored."""
    dialect = SnowflakeDialect()
    error = OSError("broken pipe")
    error.errno = 390111

    assert dialect.is_disconnect(error, None, None) is False


def test_is_disconnect_true_for_token_expired_variant():
    """390195 (authentication token expired, variant) is recoverable by
    reconnect and must be reported as a disconnect (GH #702)."""
    from snowflake.connector.errors import ProgrammingError

    dialect = SnowflakeDialect()
    error = ProgrammingError(msg="token expired", errno=390195)

    assert dialect.is_disconnect(error, None, None) is True


def test_is_disconnect_false_for_revoked_token():
    """390302 (authentication failed, token revoked) is a permanent auth
    failure — reconnecting cannot succeed, so it is intentionally NOT treated
    as a disconnect (GH #702)."""
    from snowflake.connector.errors import ProgrammingError

    dialect = SnowflakeDialect()
    error = ProgrammingError(msg="authentication failed", errno=390302)

    assert dialect.is_disconnect(error, None, None) is False


# ---------------------------------------------------------------------------
# Structured NEW_CONNECTION_PARAMETERS event
# ---------------------------------------------------------------------------


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_structured_event_is_emitted_alongside_legacy(mock_connect, fake_connection):
    """Both events are registered in order, followed by a single flush."""
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        SnowflakeDialect().connect()

    assert [event_type for event_type, _ in adapter.registered] == [
        TelemetryEvents.NEW_CONNECTION.value,
        TelemetryEvents.NEW_CONNECTION_PARAMETERS.value,
    ]
    assert adapter.flushed == 1
    # The legacy value is a str(dict); the structured value is a nested dict
    # (queryable JSON), not str(dict).
    assert isinstance(adapter.registered[0][1], str)
    assert isinstance(adapter.registered[1][1], dict)


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_structured_event_records_keys_but_never_sensitive_values(
    mock_connect, fake_connection
):
    """Every supplied option is recorded by key; only allow-listed, non-PII
    values are copied.  Credentials/identifiers appear as keys only, and the
    dialect's own injected application params are excluded entirely."""
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        SnowflakeDialect().connect(
            user="ZZuserZZ",
            password="ZZpasswordZZ",
            token="ZZtokenvalueZZ",
            private_key="ZZprivatekeyZZ",
            account="ZZaccountZZ",
            warehouse="ZZwarehouseZZ",
            role="ZZroleZZ",
            numpy=True,
            paramstyle="qmark",
            client_session_keep_alive=True,
        )

    value = _structured_value(adapter)
    conn = value["connection_parameters"]

    # Presence: customer-supplied non-credential keys are listed...
    for key in (
        "user",
        "account",
        "warehouse",
        "role",
        "numpy",
        "paramstyle",
        "client_session_keep_alive",
    ):
        assert key in conn["provided_keys"]
    # ...the dialect's injected application params are not...
    for injected in (
        "application",
        "internal_application_name",
        "internal_application_version",
    ):
        assert injected not in conn["provided_keys"]
    # ...and credential / auth-method key names are dropped entirely (CWE-532),
    # since the auth method is derivable server-side from the login request.
    for credential in ("password", "token", "private_key"):
        assert credential not in conn["provided_keys"]

    # Values: only the non-PII allow-list is copied through.
    assert conn["values"] == {
        "numpy": True,
        "paramstyle": "qmark",
        "client_session_keep_alive": True,
    }
    # No credential/identifier value leaks anywhere in the serialized payload.
    # (Sentinels are deliberately distinct from the key names so a match means
    # a real value leak, not the key appearing in ``provided_keys``.)
    serialized = str(value)
    for secret in (
        "ZZuserZZ",
        "ZZpasswordZZ",
        "ZZtokenvalueZZ",
        "ZZprivatekeyZZ",
        "ZZaccountZZ",
        "ZZwarehouseZZ",
        "ZZroleZZ",
    ):
        assert secret not in serialized


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_structured_event_records_flags_and_isolation_level(
    mock_connect, fake_connection
):
    """dialect_flags carries the config booleans plus the isolation level."""
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        SnowflakeDialect(
            isolation_level="AUTOCOMMIT",
            enable_decfloat=True,
            enable_structured_type_json=True,
        ).connect(cache_column_metadata=True)

    flags = _structured_value(adapter)["dialect_flags"]
    assert flags["isolation_level"] == "AUTOCOMMIT"
    assert flags["enable_decfloat"] is True
    assert flags["case_sensitive_identifiers"] is False
    # ``cache_column_metadata`` is a connection param, sourced from cparams.
    assert flags["cache_column_metadata"] is True
    # Structured event is a superset of the legacy NEW_CONNECTION flags.
    assert flags["enable_structured_type_json"] is True


@mock.patch.object(sqla_default.DefaultDialect, "connect")
def test_structured_event_reflects_url_driven_params(mock_connect, fake_connection):
    """URL query params resolved by create_connect_args reach the structured
    event's provided_keys/values (via the cparams passed to connect)."""
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}), _recording_adapter() as adapter:
        dialect = SnowflakeDialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query={"numpy": "True", "warehouse": "WH"},
        )
        _, cparams = dialect.create_connect_args(url)
        dialect.connect(**cparams)

    conn = _structured_value(adapter)["connection_parameters"]
    assert "numpy" in conn["provided_keys"]
    assert "warehouse" in conn["provided_keys"]
    assert conn["values"]["numpy"] is True
    # warehouse is an identifier: recorded by presence only, never by value.
    assert "warehouse" not in conn["values"]
