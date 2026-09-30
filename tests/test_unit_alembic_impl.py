#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import io
from unittest.mock import MagicMock, patch

import pytest
from alembic.ddl import impl as alembic_ddl_impl
from alembic.ddl.impl import DefaultImpl
from sqlalchemy import (
    CHAR,
    DECIMAL,
    FLOAT,
    VARCHAR,
    BigInteger,
    Column,
    DateTime,
    Double,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    MetaData,
    Numeric,
    SmallInteger,
    String,
    Table,
    Text,
)

from snowflake.sqlalchemy import TIMESTAMP_NTZ, TIMESTAMP_TZ, HybridTable
from snowflake.sqlalchemy.alembic_impl import SnowflakeImpl
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


@pytest.fixture
def dialect():
    """SnowflakeDialect without a live connection."""
    return SnowflakeDialect()


def _impl(dialect, connection=None, as_sql=False):
    return SnowflakeImpl(
        dialect, connection, as_sql, False, io.StringIO() if as_sql else None, {}
    )


def _mock_connection(dialect, *show_indexes_results):
    """A connection whose successive SHOW INDEXES calls return the given rows."""
    connection = MagicMock()
    connection.dialect = dialect
    connection.exec_driver_sql.return_value.mappings.return_value.all.side_effect = (
        list(show_indexes_results)
    )
    return connection


def _hybrid_table():
    return HybridTable(
        "my_table",
        MetaData(),
        Column("id", Integer, primary_key=True),
        Column("a", Integer, index=True),
        Column("b", Integer, index=True),
    )


class TestRegistration:
    def test_registered_for_snowflake_dialect(self, dialect):
        assert DefaultImpl.get_by_dialect(dialect) is SnowflakeImpl

    def test_subclass_defined_after_import_takes_precedence(self, dialect, monkeypatch):
        # Restore the registry afterwards; defining a class with __dialect__
        # registers it globally.
        monkeypatch.setitem(alembic_ddl_impl._impls, "snowflake", SnowflakeImpl)

        class CustomSnowflakeImpl(SnowflakeImpl):
            __dialect__ = "snowflake"

        assert DefaultImpl.get_by_dialect(dialect) is CustomSnowflakeImpl


class TestTypeSynonyms:
    @pytest.mark.parametrize(
        "metadata_type, reflected_type",
        [
            (String(), VARCHAR(16777216)),
            (Text(), VARCHAR(16777216)),
            (String(50), VARCHAR(50)),
            (CHAR(32), VARCHAR(32)),
            (Integer(), DECIMAL(38, 0)),
            (BigInteger(), DECIMAL(38, 0)),
            (SmallInteger(), DECIMAL(38, 0)),
            (Numeric(10, 2), DECIMAL(10, 2)),
            (Float(), FLOAT()),
            (Double(), FLOAT()),
            (DateTime(), TIMESTAMP_NTZ()),
            (DateTime(timezone=True), TIMESTAMP_TZ()),
        ],
    )
    def test_equivalent_types_do_not_differ(
        self, dialect, metadata_type, reflected_type
    ):
        assert not _impl(dialect).compare_type(
            Column("c", reflected_type), Column("c", metadata_type)
        )

    @pytest.mark.parametrize(
        "metadata_type, reflected_type",
        [
            (String(100), VARCHAR(50)),
            (Numeric(10, 2), DECIMAL(38, 0)),
            (Float(), DECIMAL(38, 0)),
            (String(), DECIMAL(38, 0)),
            (DateTime(), TIMESTAMP_TZ()),
        ],
    )
    def test_real_changes_differ(self, dialect, metadata_type, reflected_type):
        assert _impl(dialect).compare_type(
            Column("c", reflected_type), Column("c", metadata_type)
        )


class TestForeignKeyBackingIndexes:
    def test_fk_backing_index_is_dropped_from_comparison(self, dialect):
        reflected = Table(
            "child",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("parent_id", Integer),
            ForeignKeyConstraint(
                ["parent_id"], ["parent.id"], name="fk_child_parent_id"
            ),
        )
        backing = Index("fk_child_parent_id", reflected.c.parent_id)
        declared = Index("ix_child_parent_id", reflected.c.parent_id)
        conn_indexes = {backing, declared}

        _impl(dialect).correct_for_autogen_constraints(
            set(), conn_indexes, set(), set()
        )

        assert conn_indexes == {declared}

    def test_indexes_are_kept_without_foreign_keys(self, dialect):
        reflected = Table(
            "child",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("val", Integer),
        )
        declared = Index("ix_child_val", reflected.c.val)
        conn_indexes = {declared}

        _impl(dialect).correct_for_autogen_constraints(
            set(), conn_indexes, set(), set()
        )

        assert conn_indexes == {declared}


class TestAwaitIndexBuilds:
    def test_polls_until_all_indexes_are_active(self, dialect):
        connection = _mock_connection(
            dialect,
            [{"name": "IX_A", "status": "BUILDING"}],
            [{"name": "IX_A", "status": "ACTIVE"}],
        )
        impl = _impl(dialect, connection)
        impl.index_build_poke_interval = 0

        impl._await_index_builds(_hybrid_table())

        assert connection.exec_driver_sql.call_count == 2
        connection.exec_driver_sql.assert_called_with("SHOW INDEXES IN TABLE my_table")

    def test_raises_after_timeout(self, dialect):
        connection = _mock_connection(dialect, [{"name": "IX_A", "status": "BUILDING"}])
        impl = _impl(dialect, connection)
        impl.index_build_timeout = 0

        with pytest.raises(TimeoutError, match="IX_A"):
            impl._await_index_builds(_hybrid_table())

    def test_no_timeout_polls_until_active(self, dialect):
        building = [{"name": "IX_A", "status": "BUILDING"}]
        connection = _mock_connection(
            dialect,
            building,
            building,
            building,
            [{"name": "IX_A", "status": "ACTIVE"}],
        )
        impl = _impl(dialect, connection)
        impl.index_build_timeout = None
        impl.index_build_poke_interval = 0

        impl._await_index_builds(_hybrid_table())

        assert connection.exec_driver_sql.call_count == 4

    def test_create_index_awaits_first(self, dialect):
        impl = _impl(dialect, MagicMock())
        table = _hybrid_table()

        with patch.object(impl, "_await_index_builds") as await_builds:
            impl.create_index(Index("ix_my_table_id", table.c.id))

        await_builds.assert_called_once_with(table)

    def test_create_table_awaits_before_each_index(self, dialect):
        impl = _impl(dialect, MagicMock())
        table = _hybrid_table()

        with patch.object(impl, "_await_index_builds") as await_builds:
            impl.create_table(table)

        assert await_builds.call_count == len(table.indexes) == 2

    def test_other_ddl_does_not_await(self, dialect):
        impl = _impl(dialect, MagicMock())

        with patch.object(impl, "_await_index_builds") as await_builds:
            impl.drop_table(_hybrid_table())

        await_builds.assert_not_called()

    def test_offline_mode_emits_sql_without_polling(self, dialect):
        impl = _impl(dialect, as_sql=True)
        table = _hybrid_table()

        impl.create_index(Index("ix_my_table_id", table.c.id))

        assert "CREATE INDEX ix_my_table_id" in impl.output_buffer.getvalue()
