#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy.engine import default as sqla_default

from snowflake.sqlalchemy import snowdialect
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect, TelemetryField

# @pytest.fixture(scope="session", autouse=True)
# def init_test_schema():
#     """Override the global fixture to avoid real Snowflake connections."""
#     yield


@pytest.fixture
def fake_connection():
    return SimpleNamespace(host="example.snowflakecomputing.com", port=443)


def test_connect_sends_telemetry(monkeypatch, fake_connection):
    """Ensure telemetry is sent with the expected payload on connect."""

    monkeypatch.setattr(
        sqla_default.DefaultDialect,
        "connect",
        lambda self, *args, **kwargs: fake_connection,
    )

    rest_cls = MagicMock(name="SnowflakeRestful")
    telemetry_cls = MagicMock(name="TelemetryClient")
    telemetry_client = telemetry_cls.return_value
    payload_factory = MagicMock(name="TelemetryData.from_dict", return_value="payload")

    monkeypatch.setattr(snowdialect, "SnowflakeRestful", rest_cls)
    monkeypatch.setattr(snowdialect, "TelemetryClient", telemetry_cls)
    monkeypatch.setattr(
        snowdialect.TelemetryData,
        "from_telemetry_data_dict",
        payload_factory,
    )
    monkeypatch.setattr(snowdialect, "time_in_seconds", lambda: 123.456)
    monkeypatch.setattr(snowdialect, "VERSION", "test-version")

    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection

    rest_cls.assert_called_once_with(
        host=fake_connection.host,
        port=fake_connection.port,
        protocol="https",
        connection=fake_connection,
    )
    telemetry_cls.assert_called_once_with(rest=rest_cls.return_value)

    payload_factory.assert_called_once()
    _, kwargs = payload_factory.call_args
    assert kwargs["from_dict"] == {
        TelemetryField.KEY_TYPE.value: "sqlalchemy_version",
        TelemetryField.KEY_VALUE.value: "test-version",
    }
    assert kwargs["connection"] is fake_connection
    assert kwargs["timestamp"] == int(123.456 * 1000)

    telemetry_client.add_log_to_batch.assert_called_once_with("payload")
    telemetry_client.send_batch.assert_called_once_with()


def test_connect_logs_when_telemetry_fails(monkeypatch, caplog, fake_connection):
    """Ensure failures in telemetry do not break connect and are logged."""

    monkeypatch.setattr(
        sqla_default.DefaultDialect,
        "connect",
        lambda self, *args, **kwargs: fake_connection,
    )
    monkeypatch.setattr(
        snowdialect,
        "TelemetryClient",
        MagicMock(side_effect=RuntimeError("boom")),
    )

    caplog.set_level("DEBUG", logger="snowflake.sqlalchemy.snowdialect")

    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection
    assert any(
        "Failed to send telemetry data" in message for message in caplog.messages
    )
