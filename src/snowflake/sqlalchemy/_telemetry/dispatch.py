#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Adapter selection for the telemetry send layer.

Connector 5.x is the baseline dependency, so there is a single send adapter:
``Connector5Adapter``.  It reaches the telemetry client off the live connection
and probes that client for the arbitrary-payload API, degrading to a no-op on 5.x
builds that predate it (see ``ud``), which is why no connector-version detection
is needed here.

A fresh adapter *instance* is returned per call so each connection gets its own
client and no state leaks between connections.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .adapter import TelemetryAdapter


def get_adapter(connection) -> TelemetryAdapter:
    """Return a fresh telemetry adapter for the given connection."""
    from .ud import Connector5Adapter

    return Connector5Adapter()
