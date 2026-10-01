#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import re
import sys
import types
from unittest import mock

import pytest


class TestCreateSnowflakeAsyncEngine:
    def test_encodes_case_sensitive_schema(self):
        from snowflake.sqlalchemy.util import create_snowflake_async_engine

        with mock.patch("sqlalchemy.ext.asyncio.create_async_engine") as mock_create:
            create_snowflake_async_engine(
                "snowflake://user:pass@account/database",
                schema="MySchema",
                case_sensitive_schema=True,
            )
        called_url = mock_create.call_args[0][0]
        assert "%22MySchema%22" in called_url
        assert called_url.endswith("/database/%22MySchema%22")

    def test_same_url_as_sync_helper(self):
        from snowflake.sqlalchemy.util import (
            _snowflake_engine_url,
            create_snowflake_async_engine,
            create_snowflake_engine,
        )

        base = "snowflake://user:pass@account/database"
        with mock.patch("snowflake.sqlalchemy.util._sa_create_engine") as mock_sync:
            create_snowflake_engine(base, schema="s", case_sensitive_schema=True)
        with mock.patch("sqlalchemy.ext.asyncio.create_async_engine") as mock_async:
            create_snowflake_async_engine(base, schema="s", case_sensitive_schema=True)
        assert mock_sync.call_args[0][0] == mock_async.call_args[0][0]
        assert mock_sync.call_args[0][0] == _snowflake_engine_url(base, "s", True)


class TestAsyncRuntimeRequirements:
    """greenlet is not installed by SQLAlchemy on every platform (e.g. arm64
    macOS); without it SQLAlchemy only fails on the first connect with a
    generic ValueError, so the dialect checks for it up front."""

    def test_create_async_engine_without_greenlet_raises_hint(self, monkeypatch):
        from sqlalchemy.ext.asyncio import create_async_engine

        monkeypatch.setitem(sys.modules, "greenlet", None)
        with pytest.raises(ImportError, match=re.escape("sqlalchemy[asyncio]")):
            create_async_engine("snowflake://user:pass@account/database")

    def test_async_entry_point_without_greenlet_raises_hint(self, monkeypatch):
        from snowflake.sqlalchemy._async.async_dialect import SnowflakeDialect_async

        monkeypatch.setitem(sys.modules, "greenlet", None)
        with pytest.raises(ImportError, match="greenlet"):
            SnowflakeDialect_async.import_dbapi()

    def test_create_async_engine_with_greenlet_selects_async_dialect(self, monkeypatch):
        from sqlalchemy.ext.asyncio import create_async_engine

        from snowflake.sqlalchemy._async.async_dialect import SnowflakeDialect_async

        # Stub greenlet so the test does not depend on the runner having it
        # (it is absent on arm64 macOS); no connection is made here.
        monkeypatch.setitem(sys.modules, "greenlet", types.ModuleType("greenlet"))
        engine = create_async_engine("snowflake://user:pass@account/database")
        assert isinstance(engine.sync_engine.dialect, SnowflakeDialect_async)
