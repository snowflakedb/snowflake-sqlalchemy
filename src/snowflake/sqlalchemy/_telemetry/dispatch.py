#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Connector-version dispatch for the telemetry send layer.

Selection uses a capability probe (whether the private 5.x
``_internal.telemetry`` module is importable) rather than parsing a version
string, which is robust across betas and patch releases.  The *decision* (the
adapter class) is cached module-scope; a fresh adapter *instance* is returned
per call so each connection gets its own client and no state leaks between
connections.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .adapter import TelemetryAdapter

# Cached version decision (the adapter class), resolved once per process.
_adapter_cls: type | None = None


def _probe_adapter_cls() -> type:
    try:
        import snowflake.connector._internal.telemetry  # noqa: F401

        from .ud import Connector5Adapter

        return Connector5Adapter
    except (ImportError, AttributeError):
        from .legacy import Connector4Adapter

        return Connector4Adapter


def get_adapter(connection) -> TelemetryAdapter:
    """Return a fresh telemetry adapter for the installed connector version."""
    global _adapter_cls
    if _adapter_cls is None:
        _adapter_cls = _probe_adapter_cls()
    return _adapter_cls()


def _reset_cache() -> None:
    """Reset the cached version decision (test hook)."""
    global _adapter_cls
    _adapter_cls = None
