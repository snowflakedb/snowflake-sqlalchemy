#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""A sync-looking DBAPI facade over ``snowflake.connector.aio``.

SQLAlchemy's async dialects do not talk to an async DBAPI directly. Instead
they wrap an async driver in a set of adapter objects (defined in
``sqlalchemy.connectors.asyncio``) that expose a *synchronous* PEP-249 surface
whose blocking calls are bridged onto the running event loop via greenlets
(``await_only`` / ``await_fallback``). This is the same strategy used by the
first-party ``aiomysql``/``asyncmy``/``aiosqlite`` dialects.

This module provides:

* :class:`AsyncAdapt_snowflake_connection` – wraps an async
  ``SnowflakeConnection`` and yields adapted cursors.
* :class:`AsyncAdapt_snowflake_cursor` – wraps an async ``SnowflakeCursor``.
* :class:`AsyncAdapt_snowflake_dbapi` – the module-level shim returned from the
  dialect's ``import_dbapi()``; its ``connect()`` bridges the async connect
  coroutine and it re-exports the sync connector's constants/exceptions.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.connectors.asyncio import (
    AsyncAdapt_dbapi_connection,
    AsyncAdapt_dbapi_cursor,
    AsyncAdapt_dbapi_ss_cursor,
    AsyncAdaptFallback_dbapi_connection,
)
from sqlalchemy.util.concurrency import await_only


class _AwaitCursorFactoryMixin:
    """``connection.cursor()`` is a plain sync method as of connector
    ``5.0.0rc1`` (verified: ``inspect.iscoroutinefunction`` is ``False`` on both
    the sync and aio connection classes), but earlier drafts of the
    native-asyncio rewrite made it a coroutine. Await it when it returns an
    awaitable so a single adapter keeps working regardless of which shape the
    installed connector uses.
    """

    __slots__ = ()

    def _make_new_cursor(self, connection: Any) -> Any:
        cursor = connection.cursor()
        if hasattr(cursor, "__await__"):  # awaitable: bridge it
            return self.await_(cursor)  # type: ignore[attr-defined]
        return cursor  # already a cursor


class AsyncAdapt_snowflake_cursor(_AwaitCursorFactoryMixin, AsyncAdapt_dbapi_cursor):
    """Adapts an async ``SnowflakeCursor`` to the sync cursor protocol."""

    __slots__ = ()
    _awaitable_cursor_close = False

    def setinputsizes(self, *inputsizes: Any) -> None:
        result = self._cursor.setinputsizes(*inputsizes)
        if hasattr(result, "__await__"):
            self.await_(result)


class AsyncAdapt_snowflake_ss_cursor(
    _AwaitCursorFactoryMixin, AsyncAdapt_dbapi_ss_cursor
):
    """Server-side (streaming) variant that fetches rows lazily."""

    __slots__ = ()
    _awaitable_cursor_close = False

    def setinputsizes(self, *inputsizes: Any) -> None:
        result = self._cursor.setinputsizes(*inputsizes)
        if hasattr(result, "__await__"):
            self.await_(result)


class AsyncAdapt_snowflake_connection(AsyncAdapt_dbapi_connection):
    """Adapts an async ``SnowflakeConnection`` to the sync connection protocol."""

    __slots__ = ()

    _cursor_cls = AsyncAdapt_snowflake_cursor
    _ss_cursor_cls = AsyncAdapt_snowflake_ss_cursor

    # Convenience passthroughs the dialect / connector-aware code may read.
    @property
    def rest(self) -> Any:
        return self._connection.rest

    @property
    def sfqid(self) -> Any:
        return getattr(self._connection, "sfqid", None)

    def autocommit(self, value: bool) -> Any:
        # 5.x: Connection.autocommit is a coroutine; 4.x: plain method.
        result = self._connection.autocommit(value)
        if hasattr(result, "__await__"):
            return self.await_(result)
        return result

    def __getattr__(self, name: str) -> Any:
        """Delegate any unknown attribute (session_id, expired, etc.) to the
        underlying async connection so connector-aware dialect code keeps
        working.

        Note: coroutine methods returned this way are NOT bridged; callers that
        need blocking behaviour must use the adapter surface.
        """
        return getattr(self._connection, name)


class AsyncAdaptFallback_snowflake_connection(
    AsyncAdaptFallback_dbapi_connection, AsyncAdapt_snowflake_connection
):
    __slots__ = ()


class AsyncAdapt_snowflake_dbapi:
    """Module-level DBAPI shim returned from ``import_dbapi()``.

    Exposes ``connect()``/``paramstyle`` and re-exports every public attribute
    (exception classes, ``Binary``, type objects, error codes) from the sync
    ``snowflake.connector`` module so the existing dialect logic — which
    references ``dbapi.ProgrammingError`` and friends — continues to work.
    """

    def __init__(self, connector: Any, connector_aio: Any) -> None:
        self.connector = connector
        self.connector_aio = connector_aio
        # The connector's bind protocol is pyformat; the async cursor inherits
        # the sync cursor's paramstyle.
        self.paramstyle = getattr(connector, "paramstyle", "pyformat")

    def connect(self, *arg: Any, **kw: Any) -> AsyncAdapt_dbapi_connection:
        creator_fn = kw.pop("async_creator_fn", self.connector_aio.connect)
        if kw.pop("async_fallback", False):
            connection_cls: type[AsyncAdapt_dbapi_connection] = (
                AsyncAdaptFallback_snowflake_connection
            )
        else:
            connection_cls = AsyncAdapt_snowflake_connection
        # ``connector.aio.connect(**kw)`` returns a HybridCoroutineContextManager
        # that is awaitable; awaiting it yields the live async connection.
        return connection_cls(self, await_only(creator_fn(*arg, **kw)))

    def __getattr__(self, name: str) -> Any:
        """Fall back to the sync connector module for exceptions, Binary,
        type codes, error-code constants, etc.
        """
        return getattr(self.connector, name)
