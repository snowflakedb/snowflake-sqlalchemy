#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Async support for the Snowflake SQLAlchemy dialect.

Adapts ``snowflake.connector.aio`` onto SQLAlchemy 2.x's greenlet-bridged async
machinery. Use :func:`sqlalchemy.ext.asyncio.create_async_engine` with the same
``snowflake://`` URL as the sync dialect.
"""
