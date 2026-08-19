#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Unit tests for the connector-version-dispatch telemetry layer."""

from sys import modules
from types import SimpleNamespace
from unittest import mock

from snowflake.sqlalchemy._telemetry import (
    TelemetryEvents,
    build_connection_parameters_payload,
    build_new_connection_payload,
    dispatch,
    record_new_connection,
)
from snowflake.sqlalchemy._telemetry.legacy import Connector4Adapter
from snowflake.sqlalchemy._telemetry.ud import Connector5Adapter


def _fake_dialect(**overrides):
    """Minimal stand-in exposing the attributes the gather layer reads."""
    attrs = {
        "_case_sensitive_identifiers": False,
        "_enable_decfloat": False,
        "_enable_structured_type_json": True,
        "force_div_is_floordiv": False,
        "_isolation_level": None,
    }
    attrs.update(overrides)
    return SimpleNamespace(**attrs)


class _RecordingAdapter:
    """Adapter double that records register/flush calls for assertions."""

    def __init__(self, enabled=True):
        self._enabled = enabled
        self.registered = []
        self.flushed = 0

    def register(self, event_type, value, *, connection):
        self.registered.append((event_type, value))

    def flush(self, *, connection):
        self.flushed += 1

    def is_enabled(self, *, connection):
        return self._enabled


# ---------------------------------------------------------------------------
# dispatch: capability probe
# ---------------------------------------------------------------------------


def test_probe_selects_legacy_adapter_when_internal_telemetry_absent():
    dispatch._reset_cache()
    with mock.patch.dict(modules, {"snowflake.connector._internal.telemetry": None}):
        adapter = dispatch.get_adapter(connection=mock.MagicMock())
    assert isinstance(adapter, Connector4Adapter)
    dispatch._reset_cache()


def test_probe_selects_ud_adapter_when_internal_telemetry_present():
    dispatch._reset_cache()
    with mock.patch.dict(
        modules, {"snowflake.connector._internal.telemetry": mock.MagicMock()}
    ):
        adapter = dispatch.get_adapter(connection=mock.MagicMock())
    assert isinstance(adapter, Connector5Adapter)
    dispatch._reset_cache()


def test_get_adapter_returns_fresh_instance_per_call():
    """Version decision is cached, but a new instance is returned each call so
    each connection gets its own client (no cross-connection state leak)."""
    dispatch._reset_cache()
    with mock.patch.dict(modules, {"snowflake.connector._internal.telemetry": None}):
        a = dispatch.get_adapter(connection=mock.MagicMock())
        b = dispatch.get_adapter(connection=mock.MagicMock())
    assert a is not b
    dispatch._reset_cache()


# ---------------------------------------------------------------------------
# record_new_connection: gating, ordering, flush-on-connect
# ---------------------------------------------------------------------------


def test_record_registers_both_events_then_flushes_once():
    adapter = _RecordingAdapter(enabled=True)
    with mock.patch(
        "snowflake.sqlalchemy._telemetry.get_adapter", return_value=adapter
    ):
        record_new_connection(_fake_dialect(), mock.MagicMock(), {"numpy": True})

    assert [e for e, _ in adapter.registered] == [
        TelemetryEvents.NEW_CONNECTION.value,
        TelemetryEvents.NEW_CONNECTION_PARAMETERS.value,
    ]
    assert adapter.flushed == 1
    # Legacy event is a str(dict); structured event stays a dict.
    assert isinstance(adapter.registered[0][1], str)
    assert isinstance(adapter.registered[1][1], dict)


def test_record_does_nothing_when_disabled():
    adapter = _RecordingAdapter(enabled=False)
    with mock.patch(
        "snowflake.sqlalchemy._telemetry.get_adapter", return_value=adapter
    ):
        record_new_connection(_fake_dialect(), mock.MagicMock(), {})
    assert adapter.registered == []
    assert adapter.flushed == 0


def test_record_with_ud_adapter_is_noop_and_does_not_raise():
    with mock.patch(
        "snowflake.sqlalchemy._telemetry.get_adapter",
        return_value=Connector5Adapter(),
    ):
        # Must not raise even though nothing is sent.
        record_new_connection(_fake_dialect(), mock.MagicMock(), {"user": "x"})


# ---------------------------------------------------------------------------
# adapters
# ---------------------------------------------------------------------------


def test_connector4_is_enabled_reads_connection_flag():
    adapter = Connector4Adapter()
    assert (
        adapter.is_enabled(connection=SimpleNamespace(telemetry_enabled=False)) is False
    )
    assert (
        adapter.is_enabled(connection=SimpleNamespace(telemetry_enabled=True)) is True
    )
    # Missing attribute defaults to enabled.
    assert adapter.is_enabled(connection=SimpleNamespace()) is True


def test_connector5_is_enabled_always_true_and_send_is_noop():
    adapter = Connector5Adapter()
    conn = mock.MagicMock()
    assert adapter.is_enabled(connection=conn) is True
    assert adapter.register("t", {"a": 1}, connection=conn) is None
    assert adapter.flush(connection=conn) is None


# ---------------------------------------------------------------------------
# payloads: PII safety and shape
# ---------------------------------------------------------------------------


def test_new_connection_payload_shape():
    payload = build_new_connection_payload(_fake_dialect(_enable_decfloat=True))
    assert payload["enable_decfloat"] is True
    assert payload["case_sensitive_identifiers"] is False
    assert payload["enable_structured_type_json"] is True
    assert payload["force_div_is_floordiv"] is False
    assert "SQLAlchemy" in payload
    assert "legacy_url_params" not in payload


def test_connection_parameters_payload_flags_include_isolation_and_cache():
    dialect = _fake_dialect(
        _isolation_level="AUTOCOMMIT",
        _enable_structured_type_json=True,
    )
    payload = build_connection_parameters_payload(
        dialect, {"cache_column_metadata": True}
    )
    flags = payload["dialect_flags"]
    assert flags["isolation_level"] == "AUTOCOMMIT"
    assert flags["cache_column_metadata"] is True
    # Structured event carries the full flag set (superset of the legacy event).
    assert flags["enable_structured_type_json"] is True
    assert set(flags) == {
        "case_sensitive_identifiers",
        "enable_decfloat",
        "enable_structured_type_json",
        "cache_column_metadata",
        "force_div_is_floordiv",
        "isolation_level",
    }


def test_connection_parameters_payload_cache_metadata_defaults_false():
    """``cache_column_metadata`` is a connection param, not a dialect
    attribute: absent from ``cparams`` it reports ``False``."""
    payload = build_connection_parameters_payload(_fake_dialect(), {})
    assert payload["dialect_flags"]["cache_column_metadata"] is False


def test_connection_parameters_payload_is_pii_safe():
    cparams = {
        "user": "ZZuserZZ",
        "password": "ZZpasswordZZ",
        "account": "ZZaccountZZ",
        "warehouse": "ZZwhZZ",
        "role": "ZZroleZZ",
        "numpy": True,
        "authenticator": "https://acme.okta.com",
        "application": "injected-app",  # injected param, excluded from keys
    }
    payload = build_connection_parameters_payload(_fake_dialect(), cparams)
    conn = payload["connection_parameters"]

    # provided_keys lists supplied keys, sorted, minus injected params and
    # minus every credential / authentication-method param.
    assert conn["provided_keys"] == sorted(
        ["user", "account", "warehouse", "role", "numpy"]
    )
    assert "application" not in conn["provided_keys"]
    # Credential / auth-method key names are dropped entirely (CWE-532): even
    # their presence would reveal the auth method, which is derivable
    # server-side from the connector's login request.
    assert "password" not in conn["provided_keys"]
    assert "authenticator" not in conn["provided_keys"]

    # Only allow-listed values are copied; identifiers/credentials never.
    assert conn["values"] == {"numpy": True}
    assert "authenticator" not in conn["values"]
    serialized = str(payload)
    for secret in ("ZZuserZZ", "ZZpasswordZZ", "ZZaccountZZ", "ZZwhZZ", "ZZroleZZ"):
        assert secret not in serialized
    # The raw authenticator value (an IdP URL) never leaves the client.
    assert "acme.okta.com" not in serialized
    # ...nor does the ``authenticator`` key name appear anywhere.
    assert "authenticator" not in serialized


def test_connection_parameters_payload_drops_credential_key_names():
    """Credential / auth-method key names never reach ``provided_keys``."""
    cparams = {
        "user": "u",
        "account": "a",
        "warehouse": "wh",
        "password": "p",
        "token": "t",
        "private_key": "pk",
        "private_key_file": "/tmp/pk.p8",
        "oauth_client_secret": "s",
        "authenticator": "oauth",
        "workload_identity_provider": "AWS",
        "passcode": "123456",
    }
    conn = build_connection_parameters_payload(_fake_dialect(), cparams)[
        "connection_parameters"
    ]
    assert conn["provided_keys"] == ["account", "user", "warehouse"]
    serialized = str(conn)
    for cred in (
        "password",
        "token",
        "private_key",
        "private_key_file",
        "oauth_client_secret",
        "authenticator",
        "workload_identity_provider",
        "passcode",
    ):
        assert cred not in serialized
