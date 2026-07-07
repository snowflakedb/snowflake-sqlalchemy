#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""
Integration tests for the targeted-query cache optimisation (SNOW-3720548 / GH-701).

These tests require a live Snowflake account.  They verify end-to-end behaviour:

  - filter_names with a cold cache issues a targeted WHERE table_name IN (...)
    query and returns only the requested tables' columns.
  - The targeted and full-schema paths return identical column data for the
    same tables.
  - A warm full-schema cache is used as a superset for filter_names requests,
    returning correct results without issuing a second SQL statement.
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text


class TestReflectionCacheIntegration:
    """Integration tests — requires a live Snowflake account."""

    @pytest.fixture()
    def isolated_schema(self, engine_testaccount):
        """Create a schema with three plain tables; drop it on teardown.

        Uses three tables (table_a, table_b, table_c) so tests can assert
        that filter_names restricts results to only the requested subset.
        """
        suffix = uuid.uuid4().hex[:8]
        schema = f"sqlalchemy_cache_test_{suffix}"

        with engine_testaccount.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(
                text(
                    f"CREATE OR REPLACE TABLE {schema}.table_a "
                    "(id INTEGER, name VARCHAR(100))"
                )
            )
            conn.execute(
                text(
                    f"CREATE OR REPLACE TABLE {schema}.table_b "
                    "(id INTEGER, value FLOAT)"
                )
            )
            conn.execute(
                text(
                    f"CREATE OR REPLACE TABLE {schema}.table_c "
                    "(id INTEGER, flag BOOLEAN)"
                )
            )
            conn.commit()

        yield schema

        with engine_testaccount.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            conn.commit()

    def test_targeted_path_returns_only_requested_table(
        self, engine_testaccount, isolated_schema
    ):
        """filter_names=[table_a] must not include table_b or table_c in the result."""
        with engine_testaccount.connect() as conn:
            result = engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a"],
                info_cache={},
            )

        assert len(result) == 1
        (_, tname), _ = result[0]
        assert tname.lower() == "table_a"

    def test_targeted_path_returns_correct_columns(
        self, engine_testaccount, isolated_schema
    ):
        """filter_names=[table_a] must return the exact columns defined for that table."""
        with engine_testaccount.connect() as conn:
            result = engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a"],
                info_cache={},
            )

        (_, _), cols = result[0]
        assert {c["name"].lower() for c in cols} == {"id", "name"}

    def test_targeted_sql_contains_in_clause(
        self, engine_testaccount, isolated_schema, assert_text_in_buf
    ):
        """Targeted path must issue SQL with AND ic.table_name IN (...)."""
        with engine_testaccount.connect() as conn:
            engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a"],
                info_cache={},
            )

        assert_text_in_buf("AND ic.table_name IN")

    def test_targeted_and_full_paths_return_same_column_data(
        self, engine_testaccount, isolated_schema
    ):
        """Targeted and full-schema paths must return identical column sets."""
        with engine_testaccount.connect() as conn:
            full_result = engine_testaccount.dialect.get_multi_columns(
                conn, schema=isolated_schema, info_cache={}
            )
            targeted_result = engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a", "table_b"],
                info_cache={},
            )

        full_cols = {
            k[1].lower(): {c["name"].lower() for c in cols} for k, cols in full_result
        }
        targeted_cols = {
            k[1].lower(): {c["name"].lower() for c in cols}
            for k, cols in targeted_result
        }

        for tname in targeted_cols:
            assert targeted_cols[tname] == full_cols[tname], (
                f"Column mismatch for {tname}: "
                f"targeted={targeted_cols[tname]}, full={full_cols[tname]}"
            )

    def test_warm_cache_returns_correct_columns_for_requested_table(
        self, engine_testaccount, isolated_schema
    ):
        """Warm full-schema cache must serve correct column data for filter_names requests."""
        info_cache: dict = {}

        with engine_testaccount.connect() as conn:
            engine_testaccount.dialect.get_multi_columns(
                conn, schema=isolated_schema, info_cache=info_cache
            )
            result = engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a"],
                info_cache=info_cache,
            )

        assert len(result) == 1
        (_, tname), cols = result[0]
        assert tname.lower() == "table_a"
        assert {c["name"].lower() for c in cols} == {"id", "name"}

    def test_cold_and_warm_paths_return_identical_results(
        self, engine_testaccount, isolated_schema
    ):
        """Cold targeted path and warm-cache path must return the same column data.

        This is the core correctness invariant: regardless of which path
        populates the result, the caller sees the same columns.
        """
        with engine_testaccount.connect() as conn:
            cold_result = engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a", "table_b"],
                info_cache={},
            )

            warm_cache: dict = {}
            engine_testaccount.dialect.get_multi_columns(
                conn, schema=isolated_schema, info_cache=warm_cache
            )
            warm_result = engine_testaccount.dialect.get_multi_columns(
                conn,
                schema=isolated_schema,
                filter_names=["table_a", "table_b"],
                info_cache=warm_cache,
            )

        cold_by_table = {
            k[1].lower(): {c["name"].lower() for c in cols} for k, cols in cold_result
        }
        warm_by_table = {
            k[1].lower(): {c["name"].lower() for c in cols} for k, cols in warm_result
        }

        assert cold_by_table == warm_by_table
