#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Snowflake SQLAlchemy telemetry package.

A connector-version-dispatch telemetry layer that decouples *gathering* the
telemetry payloads (connector-agnostic, see ``payloads``) from *sending* them
(version-specific adapters selected at runtime, see ``dispatch``).
"""

from __future__ import annotations

from typing import Any

from .adapter import TelemetryAdapter, TelemetryEvents
from .dispatch import get_adapter
from .payloads import (
    build_connection_parameters_payload,
    build_new_connection_payload,
)

__all__ = [
    "TelemetryAdapter",
    "TelemetryEvents",
    "get_adapter",
    "record_new_connection",
    "build_new_connection_payload",
    "build_connection_parameters_payload",
]


def record_new_connection(
    dialect: Any, connection: Any, cparams: dict[str, Any] | None
) -> None:
    """Emit the new-connection telemetry events at connect time.

    Selects the connector-appropriate adapter, gates on the telemetry opt-in,
    registers the legacy ``NEW_CONNECTION`` event (flat, sent as ``str(dict)``
    for backward compatibility) alongside the structured
    ``NEW_CONNECTION_PARAMETERS`` event (nested dict / queryable JSON), then
    flushes the batch.  The caller wraps this in a broad ``try/except`` so any
    telemetry failure is non-fatal.
    """
    adapter = get_adapter(connection)
    if not adapter.is_enabled(connection=connection):
        return

    legacy_payload = build_new_connection_payload(dialect)
    structured_payload = build_connection_parameters_payload(dialect, cparams)

    # Legacy event keeps its historical ``str(dict)`` shape; the structured
    # event stays a dict so it lands as queryable JSON.
    adapter.register(
        TelemetryEvents.NEW_CONNECTION.value, str(legacy_payload), connection=connection
    )
    adapter.register(
        TelemetryEvents.NEW_CONNECTION_PARAMETERS.value,
        structured_payload,
        connection=connection,
    )
    adapter.flush(connection=connection)
