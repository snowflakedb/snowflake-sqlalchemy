#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Telemetry send adapter for Snowflake connector 4.x.

Uses the public 4.x telemetry API (``TelemetryClient`` / ``TelemetryData`` /
``TelemetryField``).  All connector imports are lazy (inside methods) so this
module never hard-depends on the connector telemetry surface at import time.

A fresh adapter instance is created per connection by ``dispatch.get_adapter``,
so the cached ``TelemetryClient`` below is scoped to a single connection's
``rest`` session (register x2 then flush) and never leaks across connections.
"""

from __future__ import annotations

from time import time as time_in_seconds
from typing import Any


class Connector4Adapter:
    def __init__(self) -> None:
        self._client: Any = None

    def register(self, event_type: str, value: str | dict, *, connection) -> None:
        from snowflake.connector.telemetry import (
            TelemetryClient,
            TelemetryData,
            TelemetryField,
        )

        if self._client is None:
            self._client = TelemetryClient(rest=connection.rest)

        # ``value`` is passed through verbatim: the legacy NEW_CONNECTION event
        # supplies a ``str(dict)`` while the structured NEW_CONNECTION_PARAMETERS
        # event supplies a nested dict that must stay queryable JSON.
        self._client.add_log_to_batch(
            TelemetryData.from_telemetry_data_dict(
                from_dict={
                    TelemetryField.KEY_TYPE.value: event_type,
                    TelemetryField.KEY_VALUE.value: value,
                },
                timestamp=int(time_in_seconds() * 1000),
                connection=connection,
            )
        )

    def flush(self, *, connection) -> None:
        if self._client is not None:
            self._client.send_batch()

    def is_enabled(self, *, connection) -> bool:
        # 4.x exposes a public property that already ANDs the client-side
        # setting with the server ``CLIENT_TELEMETRY_ENABLED`` parameter.
        return bool(getattr(connection, "telemetry_enabled", True))
