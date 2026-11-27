#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Async Snowflake dialect over ``snowflake.connector.aio``.

Customers should use :func:`sqlalchemy.ext.asyncio.create_async_engine` with the
same ``snowflake://`` URL as the sync dialect; SQLAlchemy selects this class
via :meth:`SnowflakeDialect.get_async_dialect_cls`.

The ``snowflake.snowflake_async`` entry point exists for the SQLAlchemy
compliance suite and other tooling that selects the async dialect via
``--dburi snowflake+snowflake_async://...``.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import pool, util
from sqlalchemy.engine.url import URL

from ..snowdialect import SnowflakeDialect
from .dbapi_shim import ASYNC_FALLBACK_REMOVED_MSG, AsyncAdapt_snowflake_dbapi

_GREENLET_HINT = (
    "Async Snowflake SQLAlchemy requires the greenlet library, which SQLAlchemy "
    "installs by default only on some platforms (not, e.g., on arm64 macOS). "
    "Install it with: pip install 'snowflake-sqlalchemy[async]' (or 'sqlalchemy[asyncio]')"
)


def _import_greenlet() -> None:
    """Import greenlet so missing SQLAlchemy[asyncio] fails with our extra hint."""
    import greenlet  # noqa: F401


def _require_async_runtime() -> None:
    """Fail at engine creation if ``greenlet`` is missing.

    Without ``greenlet``, SQLAlchemy defers the failure to the first connect and
    raises a generic ``ValueError``; raise an ``ImportError`` with the install
    command instead.
    """
    try:
        _import_greenlet()
    except ImportError as exc:
        raise ImportError(_GREENLET_HINT) from exc


class SnowflakeDialect_async(SnowflakeDialect):
    """Native-async dialect using universal-driver ``snowflake.connector.aio``."""

    driver = "snowflake"
    is_async = True
    has_terminate = False

    @classmethod
    def load_provisioning(cls) -> None:
        """Register the shared test-provisioning hooks for the async dialect.

        sqlalchemy.testing's default ``load_provisioning`` derives the package
        from this dialect's module (``snowflake.sqlalchemy._async``) and would
        look for a non-existent ``snowflake.sqlalchemy._async.provision``. Point
        it at the real provision module so the xdist follower hooks
        (``create_db``, ``follower_url_from_main``, ...) register for the async
        dialect too.
        """
        __import__("snowflake.sqlalchemy.provision")

    @classmethod
    def import_dbapi(cls) -> AsyncAdapt_snowflake_dbapi:
        _require_async_runtime()
        from snowflake.connector import aio as connector_aio

        from snowflake import connector

        return AsyncAdapt_snowflake_dbapi(connector, connector_aio)

    @classmethod
    def get_pool_class(cls, url: URL) -> Any:
        async_fallback = url.query.get("async_fallback", False)
        if util.asbool(async_fallback):
            # async_fallback (and FallbackAsyncAdaptedQueuePool) was removed in
            # SQLAlchemy 2.1. Raise the same error as connect() so URL handling
            # and the DBAPI shim stay consistent across 2.0 and 2.1.
            fallback_pool = getattr(pool, "FallbackAsyncAdaptedQueuePool", None)
            if fallback_pool is None:
                raise NotImplementedError(ASYNC_FALLBACK_REMOVED_MSG)
            return fallback_pool
        return pool.AsyncAdaptedQueuePool

    def get_driver_connection(self, connection: Any) -> Any:
        return connection._connection

    def _log_new_connection_event(
        self, connection: Any, cparams: dict | None = None
    ) -> None:
        # Sync telemetry uses connector REST handles that are not valid on aio.
        return


dialect = SnowflakeDialect_async
