#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Telemetry send adapter for Snowflake connector 5.x / Universal Driver.

Connector 5.x does not expose a public arbitrary-payload telemetry API: the
public ``snowflake.connector.telemetry`` module is a backward-compat stub, and
the real implementation lives in the private ``_internal.telemetry`` with a
fixed event vocabulary (``send_api_usage`` / ``send_wrapper_error``).

Per the Universal Driver telemetry guidance, libraries must NOT depend on
connector private internals.  This adapter is therefore a deliberate no-op that
keeps the gather path intact and serves as the extension seam for when the
connector exposes a public ``send_telemetry(event_type, payload)`` API:
  1. Implement ``register`` to call that public API.
  2. Update ``dispatch`` to detect and route to it.
"""

from __future__ import annotations


class Connector5Adapter:
    def register(self, event_type: str, value: str | dict, *, connection) -> None:
        # No-op: no public UD telemetry-payload API exists yet. See module docstring.
        return None

    def flush(self, *, connection) -> None:
        return None

    def is_enabled(self, *, connection) -> bool:
        # 5.x does not expose a public opt-in check; gating is server-side.
        return True
