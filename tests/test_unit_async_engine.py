#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest import mock


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
