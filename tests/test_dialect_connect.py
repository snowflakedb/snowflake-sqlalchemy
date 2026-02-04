#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sys import modules
from types import SimpleNamespace
from unittest import mock

import pytest
from sqlalchemy import __version__ as SQLALCHEMY_VERSION
from sqlalchemy.engine import default as sqla_default

from snowflake.sqlalchemy.snowdialect import (
    SnowflakeDialect,
    TelemetryEvents,
    TelemetryField,
)


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
        {"SQLAlchemy": SQLALCHEMY_VERSION}
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

    assert telemetry_value == str({"SQLAlchemy": SQLALCHEMY_VERSION, "pandas": "2.1.0"})


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

    assert telemetry_value == str({"SQLAlchemy": SQLALCHEMY_VERSION})


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
