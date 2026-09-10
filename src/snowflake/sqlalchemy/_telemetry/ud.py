#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Telemetry send adapter for Snowflake connector 5.x / Universal Driver.

Connector 5.x keeps its telemetry client private and does not use the public
``snowflake.connector.telemetry`` module (a backward-compat stub whose
``TelemetryClient`` raises ``NotImplementedError``).  The real client lives at
``snowflake.connector._common.telemetry`` from rc2 onwards (earlier 5.x betas
shipped it at ``_internal.telemetry``) and is reached off a live connection via
the ``_telemetry`` backward-compat accessor.

For *derived software* (a SQLAlchemy dialect, Snowpark, the CLI), that client
exposes an arbitrary-payload channel -- ``try_add_log_to_batch`` /
``add_log_to_batch`` -- which forwards a caller-produced JSON message to the
native core (``core_driver.telemetry_send_log``); the core owns batching and
egress, so there is no client-side batch to flush.  This is distinct from the
connector's *own* instrumentation path (``send_api_usage`` /
``send_wrapper_error``), which carries a fixed, names-only vocabulary and is not
suitable for the dialect's structured connection payloads.

The payload shape mirrors the historical 4.x wire format --
``{"type": event_type, "value": value}`` -- so events stay comparable with
telemetry emitted by earlier dialect releases.

The arbitrary-payload API is only present on newer connectors.  When the
resolved client lacks it (5.x builds older than the pinned rc2 floor),
``register`` degrades to a no-op so the gather path stays intact and nothing
raises; it activates automatically once a connector exposing the API is
installed.
"""

from __future__ import annotations

from time import time as time_in_seconds
from typing import Any


def _resolve_client(connection: Any) -> Any | None:
    """Return the connection's telemetry client, or ``None`` if it cannot send.

    Prefers the ``_telemetry`` backward-compat accessor, falling back to the
    concrete ``_telemetry_client`` attribute.  Only returns a candidate that
    actually exposes the arbitrary-payload send API, so connectors that predate
    it (or expose ``_telemetry`` as something else) resolve to ``None`` and the
    caller no-ops.
    """
    for attr in ("_telemetry", "_telemetry_client"):
        client = getattr(connection, attr, None)
        if client is not None and (
            hasattr(client, "try_add_log_to_batch")
            or hasattr(client, "add_log_to_batch")
        ):
            return client
    return None


class _TelemetryPayload:
    """Duck-typed fallback matching the ``add_log_to_batch`` contract.

    The connector's ``add_log_to_batch`` only reads ``.message`` (JSON-encoded)
    and ``.timestamp``; this stand-in is used when the connector's own
    ``TelemetryData`` cannot be imported (e.g. older 5.x layouts), so the send
    path never hard-depends on that symbol.
    """

    __slots__ = ("message", "timestamp")

    def __init__(self, message: dict, timestamp: int) -> None:
        self.message = message
        self.timestamp = timestamp


def _build_telemetry_data(event_type: str, value: str | dict, connection) -> Any:
    """Build a connector-format telemetry entry, mirroring the 4.x payload.

    Keys match the historical 4.x wire format (``type`` / ``value``) so events
    stay comparable across dialect releases.  ``value`` is passed through
    verbatim: the legacy NEW_CONNECTION event supplies a ``str(dict)`` while the
    structured NEW_CONNECTION_PARAMETERS event supplies a nested dict that must
    stay queryable JSON.
    """
    from_dict = {"type": event_type, "value": value}
    timestamp = int(time_in_seconds() * 1000)
    try:
        from snowflake.connector._common.telemetry import TelemetryData

        return TelemetryData.from_telemetry_data_dict(
            from_dict=from_dict, timestamp=timestamp, connection=connection
        )
    except Exception:
        return _TelemetryPayload(from_dict, timestamp)


class Connector5Adapter:
    def register(self, event_type: str, value: str | dict, *, connection) -> None:
        client = _resolve_client(connection)
        if client is None:
            # 5.x connector without the derived-software payload API; nothing to
            # send.  Kept non-fatal so the gather path is unaffected.
            return

        telemetry_data = _build_telemetry_data(event_type, value, connection)

        # Fire-and-forget: the core sends immediately over RPC and swallows
        # transport errors, so a single failed event never breaks connect.
        send = getattr(client, "try_add_log_to_batch", None) or client.add_log_to_batch
        send(telemetry_data)

    def flush(self, *, connection) -> None:
        # No-op: the native core owns batching and flush (threshold / connection
        # release), so there is no client-side batch to send.
        return None

    def is_enabled(self, *, connection) -> bool:
        # 5.x exposes the same public property as 4.x; it ANDs the client-side
        # flag with the server ``CLIENT_TELEMETRY_ENABLED`` parameter.
        return bool(getattr(connection, "telemetry_enabled", True))
