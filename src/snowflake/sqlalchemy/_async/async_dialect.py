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

import sys
from typing import Any

from sqlalchemy import pool, util
from sqlalchemy.engine.url import URL

from ..snowdialect import SnowflakeDialect
from .dbapi_shim import AsyncAdapt_snowflake_dbapi

_MIN_PYTHON = (3, 10)
_ASYNC_EXTRA_HINT = (
    "Async Snowflake SQLAlchemy requires Python >= 3.10 and "
    "snowflake-connector-python 5.x. Install with: "
    "pip install 'snowflake-sqlalchemy[async]'"
)


def _require_async_runtime() -> None:
    if sys.version_info < _MIN_PYTHON:
        raise ImportError(_ASYNC_EXTRA_HINT)
    try:
        from snowflake.connector import aio  # noqa: F401
    except ImportError as exc:
        raise ImportError(_ASYNC_EXTRA_HINT) from exc


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
            return pool.FallbackAsyncAdaptedQueuePool
        return pool.AsyncAdaptedQueuePool

    def get_driver_connection(self, connection: Any) -> Any:
        return connection._connection

    def _log_new_connection_event(
        self, connection: Any, cparams: dict | None = None
    ) -> None:
        # Sync telemetry uses connector REST handles that are not valid on aio.
        return


dialect = SnowflakeDialect_async
