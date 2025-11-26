#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from types import SimpleNamespace
from unittest import mock

import pytest
from sqlalchemy import __version__ as SQLALCHEMY_VERSION
from sqlalchemy.engine import default as sqla_default

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect, TelemetryField


@pytest.fixture
def fake_connection():
    return SimpleNamespace(
        host="example.snowflakecomputing.com",
        port=443,
        application="test_app",
    )


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
@mock.patch("snowflake.sqlalchemy.snowdialect.SnowflakeRestful")
def test_connect_sends_telemetry(
    mock_restful, mock_telemetry_client, mock_connect, fake_connection
):
    """Ensure telemetry is sent with the expected payload on connect."""
    mock_connect.return_value = fake_connection

    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection

    # Verify add_log_to_batch was called with correct payload
    telemetry_instance = mock_telemetry_client.return_value
    payload = telemetry_instance.add_log_to_batch.call_args[0][0]
    assert payload.message[TelemetryField.KEY_TYPE.value] == "sqlalchemy_version"
    assert payload.message[TelemetryField.KEY_VALUE.value] == SQLALCHEMY_VERSION
    assert payload.timestamp != 0

    # Verify send_batch was called
    telemetry_instance.send_batch.assert_called_once()


@mock.patch.object(sqla_default.DefaultDialect, "connect")
@mock.patch("snowflake.sqlalchemy.snowdialect.TelemetryClient")
@mock.patch("snowflake.sqlalchemy.snowdialect.SnowflakeRestful")
def test_connect_logs_when_telemetry_fails(
    mock_restful, mock_telemetry_client, mock_connect, caplog, fake_connection
):
    """Ensure failures in telemetry do not break connect and are logged."""
    mock_connect.return_value = fake_connection
    mock_telemetry_client.side_effect = RuntimeError("boom")

    caplog.set_level("DEBUG", logger="snowflake.sqlalchemy.snowdialect")

    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection
    assert any(
        "Failed to send telemetry data" in message for message in caplog.messages
    )
