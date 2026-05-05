#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sys import modules
from types import SimpleNamespace
from unittest import mock

import pytest
from sqlalchemy import __version__ as SQLALCHEMY_VERSION
from sqlalchemy.engine import default as sqla_default
from sqlalchemy.engine.url import URL as SAUrl

from snowflake.sqlalchemy.snowdialect import (
    SnowflakeDialect,
    TelemetryEvents,
    TelemetryField,
)

#: Default flag values emitted by ``_log_new_connection_event`` when the
#: dialect is constructed without overrides.  Kept here so tests can extend
#: this dict incrementally rather than repeating the full payload shape.
_DEFAULT_FLAG_PAYLOAD = {
    "case_sensitive_identifiers": False,
    "enable_decfloat": False,
    "cache_column_metadata": False,
    "force_div_is_floordiv": True,
}


@pytest.fixture
def fake_connection():
    return SimpleNamespace(
        host="example.snowflakecomputing.com",
        port=443,
        application="test_app",
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
def test_connect_sends_telemetry(mock_telemetry_client, mock_connect, fake_connection):
    """Ensure telemetry is sent with the expected payload on connect."""
    mock_connect.return_value = fake_connection

    # Mock out pandas to ensure deterministic behavior
    with mock.patch.dict(modules, {"pandas": None}):
        fake_connection.rest = mock.MagicMock()
        dialect = SnowflakeDialect()
        result = dialect.connect()

    assert result is fake_connection

    # Verify add_log_to_batch was called with correct payload
    telemetry_instance = mock_telemetry_client.return_value

    payload = telemetry_instance.add_log_to_batch.call_args[0][0]
    assert (
        payload.message[TelemetryField.KEY_TYPE.value]
        == TelemetryEvents.NEW_CONNECTION.value
    )
    assert payload.message[TelemetryField.KEY_VALUE.value] == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **_DEFAULT_FLAG_PAYLOAD}
    )
    assert payload.timestamp != 0

    # Verify send_batch was called
    telemetry_instance.send_batch.assert_called_once()


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
def test_connect_telemetry_includes_pandas_when_available(
    mock_telemetry_client, mock_connect, fake_connection
):
    """Ensure telemetry includes pandas version when pandas is installed."""
    mock_connect.return_value = fake_connection

    # Create a mock pandas module with a version
    mock_pandas = mock.MagicMock()
    mock_pandas.__version__ = "2.1.0"

    with mock.patch.dict(modules, {"pandas": mock_pandas}):
        fake_connection.rest = mock.MagicMock()
        dialect = SnowflakeDialect()
        dialect.connect()

    telemetry_instance = mock_telemetry_client.return_value
    payload = telemetry_instance.add_log_to_batch.call_args[0][0]
    telemetry_value = payload.message[TelemetryField.KEY_VALUE.value]

    assert telemetry_value == str(
        {
            "SQLAlchemy": SQLALCHEMY_VERSION,
            "pandas": "2.1.0",
            **_DEFAULT_FLAG_PAYLOAD,
        }
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
def test_connect_telemetry_excludes_pandas_when_not_available(
    mock_telemetry_client, mock_connect, fake_connection
):
    """Ensure telemetry does not include pandas when it is not installed."""
    mock_connect.return_value = fake_connection

    # Simulate pandas not being installed
    with mock.patch.dict(modules, {"pandas": None}):
        fake_connection.rest = mock.MagicMock()
        dialect = SnowflakeDialect()
        dialect.connect()

    telemetry_instance = mock_telemetry_client.return_value
    payload = telemetry_instance.add_log_to_batch.call_args[0][0]
    telemetry_value = payload.message[TelemetryField.KEY_VALUE.value]

    assert telemetry_value == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **_DEFAULT_FLAG_PAYLOAD}
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
def test_connect_logs_when_telemetry_fails(
    mock_telemetry_client, mock_connect, caplog, fake_connection
):
    """Ensure failures in telemetry do not break connect and are logged."""
    mock_connect.return_value = fake_connection
    mock_telemetry_client.side_effect = RuntimeError("boom")

    caplog.set_level("DEBUG", logger="snowflake.sqlalchemy.snowdialect")

    fake_connection.rest = mock.MagicMock()
    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection
    assert any(
        "Failed to send telemetry data" in message for message in caplog.messages
    )


# ---------------------------------------------------------------------------
# Dialect flags are recorded in the NEW_CONNECTION telemetry payload
# ---------------------------------------------------------------------------


def _telemetry_payload(dialect, telemetry_client_mock, fake_connection):
    """Run ``dialect.connect()`` once and return the payload message dict."""
    fake_connection.rest = mock.MagicMock()
    dialect.connect()
    payload = telemetry_client_mock.return_value.add_log_to_batch.call_args[0][0]
    return payload.message


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
            {"cache_column_metadata": True},
            {"cache_column_metadata": True},
        ),
        (
            {"force_div_is_floordiv": False},
            {"force_div_is_floordiv": False},
        ),
        (
            # All flags flipped at once
            {
                "case_sensitive_identifiers": True,
                "enable_decfloat": True,
                "cache_column_metadata": True,
                "force_div_is_floordiv": False,
            },
            {
                "case_sensitive_identifiers": True,
                "enable_decfloat": True,
                "cache_column_metadata": True,
                "force_div_is_floordiv": False,
            },
        ),
    ],
    ids=[
        "case_sensitive_identifiers_true",
        "enable_decfloat_true",
        "cache_column_metadata_true",
        "force_div_is_floordiv_false",
        "all_flipped",
    ],
)
@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
def test_connect_telemetry_records_kwarg_flags(
    mock_telemetry_client,
    mock_connect,
    fake_connection,
    ctor_kwargs,
    expected_overrides,
):
    """Constructor-kwarg flags reach the telemetry payload unchanged."""
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}):
        dialect = SnowflakeDialect(**ctor_kwargs)
        message = _telemetry_payload(dialect, mock_telemetry_client, fake_connection)

    expected_flags = {**_DEFAULT_FLAG_PAYLOAD, **expected_overrides}
    assert message[TelemetryField.KEY_VALUE.value] == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **expected_flags}
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
def test_connect_telemetry_records_url_driven_flag(
    mock_telemetry_client, mock_connect, fake_connection
):
    """A URL-driven flag is reflected on the first new-connection event.

    ``create_connect_args`` runs before ``connect()`` returns, and the
    telemetry event is emitted *after* the connection is established.  The
    flag value captured on the event must therefore be the post-URL value,
    not the constructor default.
    """
    mock_connect.return_value = fake_connection

    with mock.patch.dict(modules, {"pandas": None}):
        dialect = SnowflakeDialect()
        url = SAUrl.create(
            "snowflake",
            username="u",
            password="p",
            host="testaccount",
            query={"case_sensitive_identifiers": "True"},
        )
        # Trigger the URL-parameter application prior to connect().
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            dialect.create_connect_args(url)

        message = _telemetry_payload(dialect, mock_telemetry_client, fake_connection)

    expected_flags = {**_DEFAULT_FLAG_PAYLOAD, "case_sensitive_identifiers": True}
    assert message[TelemetryField.KEY_VALUE.value] == str(
        {"SQLAlchemy": SQLALCHEMY_VERSION, **expected_flags}
    )
