#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy import pool
from sqlalchemy.engine.url import make_url

from snowflake.sqlalchemy._async.async_dialect import (
    _GREENLET_HINT,
    SnowflakeDialect_async,
    _import_greenlet,
    _require_async_runtime,
)
from snowflake.sqlalchemy._async.dbapi_shim import (
    ASYNC_FALLBACK_REMOVED_MSG,
    AsyncAdapt_snowflake_dbapi,
    AsyncAdaptFallback_snowflake_connection,
)
from snowflake.sqlalchemy.base import SnowflakeTypeCompiler
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

_PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


class TestAsyncioExtra:
    def test_install_hint(self):
        assert "snowflake-sqlalchemy[async]" in _GREENLET_HINT

    def test_pyproject_async_extra_pulls_sqlalchemy_asyncio(self):
        import tomllib

        data = tomllib.loads(_PYPROJECT.read_text())
        extras = data["project"]["optional-dependencies"]["async"]
        assert any("SQLAlchemy[asyncio]" in dep for dep in extras)

    def test_require_async_runtime_missing_greenlet(self, monkeypatch):
        def _missing() -> None:
            raise ImportError("greenlet is not installed")

        monkeypatch.setattr(
            "snowflake.sqlalchemy._async.async_dialect._import_greenlet",
            _missing,
        )
        with pytest.raises(ImportError, match=r"snowflake-sqlalchemy\[async\]"):
            _require_async_runtime()

    def test_import_greenlet_is_callable(self):
        pytest.importorskip("greenlet")
        _import_greenlet()


class TestAsyncFallback:
    def test_pool_without_fallback_flag(self):
        url = make_url("snowflake://user:pass@account")
        assert SnowflakeDialect_async.get_pool_class(url) is pool.AsyncAdaptedQueuePool

    def test_pool_and_connect_agree_when_fallback_requested(self):
        url = make_url("snowflake://user:pass@account?async_fallback=true")
        fallback_pool = getattr(pool, "FallbackAsyncAdaptedQueuePool", None)
        dbapi = AsyncAdapt_snowflake_dbapi(mock.Mock(), mock.Mock())

        if fallback_pool is None:
            with pytest.raises(NotImplementedError, match="async_fallback"):
                SnowflakeDialect_async.get_pool_class(url)
            with pytest.raises(NotImplementedError, match="async_fallback"):
                dbapi.connect(async_fallback=True)
            assert AsyncAdaptFallback_snowflake_connection is None
        else:
            assert SnowflakeDialect_async.get_pool_class(url) is fallback_pool
            assert AsyncAdaptFallback_snowflake_connection is not None
            creator = mock.Mock(return_value=mock.Mock())
            with mock.patch(
                "snowflake.sqlalchemy._async.dbapi_shim.await_fallback",
                side_effect=lambda value: value,
            ):
                conn = dbapi.connect(async_creator_fn=creator, async_fallback=True)
            assert isinstance(conn, AsyncAdaptFallback_snowflake_connection)

    @pytest.mark.skipif(
        AsyncAdaptFallback_snowflake_connection is None,
        reason="async_fallback was removed in SQLAlchemy 2.1",
    )
    def test_fallback_connection_bridges_without_greenlet_context(self):
        """SA 2.0 fallback adapters must use await_fallback, not await_only.

        Runs outside any greenlet spawn, where await_only would raise
        MissingGreenlet; await_fallback drives the coroutine on a loop instead.
        """
        calls = []

        class _FakeCursor:
            async def __aenter__(self):
                return self

            async def setinputsizes(self, *sizes):
                calls.append(("setinputsizes", sizes))

        class _FakeConnection:
            def cursor(self):
                return _FakeCursor()

            async def autocommit(self, value):
                calls.append(("autocommit", value))
                return value

        async def creator(*arg, **kw):
            return _FakeConnection()

        dbapi = AsyncAdapt_snowflake_dbapi(mock.Mock(), mock.Mock())
        conn = dbapi.connect(async_creator_fn=creator, async_fallback=True)
        assert isinstance(conn, AsyncAdaptFallback_snowflake_connection)

        assert conn.autocommit(True) is True
        conn.cursor().setinputsizes(1)
        assert calls == [("autocommit", True), ("setinputsizes", (1,))]

    def test_removed_message_is_stable(self):
        assert "SQLAlchemy 2.1" in ASYNC_FALLBACK_REMOVED_MSG


class TestAggregateOrderByStyle:
    def test_within_group_when_sa21_api_present(self):
        try:
            from sqlalchemy.sql.compiler import AggregateOrderByStyle
        except ImportError:
            pytest.skip("aggregate_order_by_style is SQLAlchemy 2.1+")
        assert (
            SnowflakeDialect.aggregate_order_by_style
            is AggregateOrderByStyle.WITHIN_GROUP
        )

    def test_attribute_absent_on_sa20(self):
        try:
            from sqlalchemy.sql.compiler import AggregateOrderByStyle  # noqa: F401
        except ImportError:
            assert not hasattr(SnowflakeDialect, "aggregate_order_by_style")
        else:
            pytest.skip("running on SQLAlchemy 2.1+")


class TestStringTypeCollationSignature:
    def test_render_string_type_accepts_sa21_collation_kwargs(self):
        compiler = SnowflakeTypeCompiler(SnowflakeDialect())
        rendered = compiler._render_string_type(
            "VARCHAR",
            100,
            "en-ci",
            "ignored_schema",
            object(),
        )
        assert rendered == "VARCHAR(100) COLLATE 'en-ci'"


class TestReflectedIntegerType:
    def test_int_and_integer_map_to_generic_integer(self):
        """INT and INTEGER reflect as generic ``Integer``.

        SA 2.1's TableViaSelectTest compares reflected types to Integer;
        INTEGER is a distinct class there and must not be used in ischema_names.
        """
        from sqlalchemy.sql.sqltypes import INTEGER, Integer

        from snowflake.sqlalchemy.parser.custom_type_parser import ischema_names

        assert ischema_names["INT"] is Integer
        assert ischema_names["INTEGER"] is Integer
        # Star-import / inspect tests still need the INTEGER *name* on the parser.
        import snowflake.sqlalchemy.parser.custom_type_parser as parser

        assert parser.INTEGER is INTEGER

    def test_number_scale_zero_affinity_is_generic_integer(self):
        from sqlalchemy.sql.sqltypes import Integer

        from snowflake.sqlalchemy.custom_types import _CUSTOM_DECIMAL

        assert _CUSTOM_DECIMAL(precision=38, scale=0)._type_affinity is Integer
        assert _CUSTOM_DECIMAL(precision=10, scale=2)._type_affinity is not Integer
