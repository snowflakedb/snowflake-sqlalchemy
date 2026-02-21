#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from unittest.mock import patch

import pytest
from sqlalchemy import Column, Integer, Sequence, String, inspect
from sqlalchemy.orm import declarative_base

from snowflake.sqlalchemy.custom_types import OBJECT


@pytest.mark.parametrize(
    "cache_column_metadata,expected_schema_count,expected_desc_count",
    [
        (False, 0, 1),
        (True, 1, 3),
    ],
)
def test_cache_column_metadata(
    cache_column_metadata,
    expected_schema_count,
    expected_desc_count,
    engine_testaccount,
):
    """
    Test cache_column_metadata behavior for column reflection.

    This test verifies that the _cache_column_metadata flag controls whether
    the dialect prefetches all columns from a schema or queries individual tables.

    When cache_column_metadata=False (default):
    - _get_schema_columns is NOT called
    - Only the requested table is queried via DESC TABLE
    - Results in 1 DESC call for the User table

    When cache_column_metadata=True:
    - _get_schema_columns IS called (fetches all columns via information_schema)
    - Additional DESC TABLE calls are made for tables with structured types
      (MAP, ARRAY, OBJECT) to get detailed type information
    - Results in 1 schema query + 3 DESC calls (User, OtherTableA, OtherTableB)

    Note: OtherTableC does not trigger a DESC call because it has no structured types.
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        object = Column(OBJECT)

    class OtherTableA(Base):
        __tablename__ = "other_a"

        id = Column(Integer, primary_key=True)
        name = Column(String)
        payload = Column(OBJECT)

    class OtherTableB(Base):
        __tablename__ = "other_b"

        id = Column(Integer, primary_key=True)
        name = Column(String)
        payload = Column(OBJECT)

    class OtherTableC(Base):
        __tablename__ = "other_c"

        id = Column(Integer, primary_key=True)
        name = Column(String)

    models = [User, OtherTableA, OtherTableB, OtherTableC]

    Base.metadata.create_all(engine_testaccount)

    try:
        inspector = inspect(engine_testaccount)
        schema = inspector.default_schema_name

        # Verify cache_column_metadata is False by default
        assert not engine_testaccount.dialect._cache_column_metadata

        # Track calls to _get_schema_columns
        schema_columns_count = []
        original_schema_columns = engine_testaccount.dialect._get_schema_columns

        def tracked_schema_columns(*args, **kwargs):
            """Wrapper to count calls to _get_schema_columns."""
            schema_columns_count.append(1)
            return original_schema_columns(*args, **kwargs)

        # Track DESC TABLE commands executed by the dialect
        desc_call_count = []

        def tracked_execute(statement, *args, **kwargs):
            """
            Wrapper to count DESC TABLE commands for our test tables.

            Only counts DESC commands with the sqlalchemy:_get_schema_columns comment
            that target one of our test tables (filters out unrelated DESC calls).
            """
            stmt_str = str(statement)
            stmt_upper = stmt_str.upper()
            if (
                "DESC" in stmt_str
                and "sqlalchemy:_get_schema_columns" in stmt_str
                and any(model.__tablename__.upper() in stmt_upper for model in models)
            ):
                desc_call_count.append(stmt_str)
            return original_execute(statement, *args, **kwargs)

        with patch.object(
            engine_testaccount.dialect,
            "_cache_column_metadata",
            cache_column_metadata,
        ), patch.object(
            engine_testaccount.dialect,
            "_get_schema_columns",
            side_effect=tracked_schema_columns,
        ):
            with engine_testaccount.connect() as conn:
                original_execute = conn.execute

                with patch.object(conn, "execute", side_effect=tracked_execute):
                    tracked_inspector = inspect(conn)

                    # Reflect columns for User table
                    _ = tracked_inspector.get_columns(User.__tablename__, schema)

                    # Verify expected behavior based on cache_column_metadata setting
                    assert (
                        len(schema_columns_count) == expected_schema_count
                    ), f"Expected {expected_schema_count} _get_schema_columns call(s), got {len(schema_columns_count)}"
                    assert (
                        len(desc_call_count) == expected_desc_count
                    ), f"Expected {expected_desc_count} DESC call(s), got {len(desc_call_count)}"
    finally:
        Base.metadata.drop_all(engine_testaccount)
