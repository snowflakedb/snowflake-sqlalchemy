#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

from enum import Enum
from typing import Protocol, runtime_checkable


class TelemetryEvents(Enum):
    """Events emitted by the SQLAlchemy telemetry layer."""

    NEW_CONNECTION = "sqlalchemy_new_connection"
    NEW_CONNECTION_PARAMETERS = "sqlalchemy_new_connection_parameters"


@runtime_checkable
class TelemetryAdapter(Protocol):
    """Connector-version-agnostic telemetry sending contract.

    ``register`` queues an event; ``flush`` sends the batch.  This split keeps
    the connector-agnostic "gather" step (building payloads) decoupled from the
    version-specific "send" step.
    """

    def register(self, event_type: str, value: str | dict, *, connection) -> None:
        """Queue a telemetry event for the given connection."""
        ...

    def flush(self, *, connection) -> None:
        """Send all queued telemetry events for the given connection."""
        ...

    def is_enabled(self, *, connection) -> bool:
        """Return whether telemetry should be sent for this connection."""
        ...
