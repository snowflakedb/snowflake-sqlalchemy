#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

"""Tests for telemetry behavior during SnowflakeDialect.connect()."""

import pytest
from sqlalchemy.engine import default

from snowflake.sqlalchemy import snowdialect
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect, TelemetryField


@pytest.fixture(scope="session", autouse=True)
def init_test_schema():
    """Override the global fixture to avoid real Snowflake connections."""
    yield


@pytest.fixture
def fake_connection():
    class _FakeConnection:
        host = "example.snowflakecomputing.com"
        port = 443

    return _FakeConnection()


def test_connect_sends_telemetry(monkeypatch, fake_connection):
    """Ensure telemetry is sent with the expected payload on connect."""

    captured = {}

    # Prevent real connections
    monkeypatch.setattr(
        default.DefaultDialect,
        "connect",
        lambda self, *args, **kwargs: fake_connection,
    )

    class FakeRestClient:
        def __init__(self, host, port, protocol, connection):
            captured["rest_init"] = {
                "host": host,
                "port": port,
                "protocol": protocol,
                "connection": connection,
            }
            captured["rest_instance"] = self

    class FakeTelemetryClient:
        def __init__(self, rest):
            captured["rest_passed"] = rest

        def add_log_to_batch(self, data):
            captured["log_data"] = data

        def send_batch(self):
            captured["send_batch"] = captured.get("send_batch", 0) + 1

    def fake_from_telemetry_data_dict(*, from_dict, timestamp, connection):
        captured["payload"] = from_dict
        captured["timestamp"] = timestamp
        captured["connection"] = connection
        return {"from_dict": from_dict}

    monkeypatch.setattr(snowdialect, "SnowflakeRestful", FakeRestClient)
    monkeypatch.setattr(snowdialect, "TelemetryClient", FakeTelemetryClient)
    monkeypatch.setattr(
        snowdialect.TelemetryData,
        "from_telemetry_data_dict",
        staticmethod(fake_from_telemetry_data_dict),
    )
    monkeypatch.setattr(snowdialect, "time_in_seconds", lambda: 123.456)
    monkeypatch.setattr(snowdialect, "VERSION", "test-version")

    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection
    assert captured["rest_init"] == {
        "host": fake_connection.host,
        "port": fake_connection.port,
        "protocol": "https",
        "connection": fake_connection,
    }
    assert captured["rest_passed"] is captured["rest_instance"]
    assert captured["send_batch"] == 1
    assert captured["connection"] is fake_connection
    assert captured["payload"] == {
        TelemetryField.KEY_TYPE.value: "sqlalchemy_version",
        TelemetryField.KEY_VALUE.value: "test-version",
    }
    assert captured["timestamp"] == int(123.456 * 1000)


def test_connect_logs_when_telemetry_fails(monkeypatch, caplog, fake_connection):
    """Ensure failures in telemetry do not break connect and are logged."""

    monkeypatch.setattr(
        default.DefaultDialect,
        "connect",
        lambda self, *args, **kwargs: fake_connection,
    )

    class BrokenTelemetryClient:
        def __init__(self, rest):
            raise RuntimeError("boom")

    monkeypatch.setattr(snowdialect, "TelemetryClient", BrokenTelemetryClient)

    caplog.set_level("DEBUG", logger="snowflake.sqlalchemy.snowdialect")

    dialect = SnowflakeDialect()
    result = dialect.connect()

    assert result is fake_connection
    assert any(
        "Failed to send telemetry data" in message for message in caplog.messages
    )
