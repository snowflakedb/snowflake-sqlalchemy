#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest
from sqlalchemy import Column, Integer, MetaData, String, select, text
from sqlalchemy.exc import ProgrammingError

from snowflake.sqlalchemy import SnowflakeTable

CURRENT_TRANSACTION = text("SELECT CURRENT_TRANSACTION()")


@pytest.fixture
def txn_table(engine_testaccount, db_table_name):
    metadata = MetaData()
    table = SnowflakeTable(
        db_table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )
    # Table names are unique per test, so skip the has_table() probe: its
    # expected "does not exist" error intermittently segfaults the connector
    # when closing the cursor under pytest-xdist with 4+ workers.
    metadata.create_all(engine_testaccount, checkfirst=False)
    try:
        yield table
    finally:
        metadata.drop_all(engine_testaccount)


def _ids(engine, table):
    with engine.connect() as conn:
        return [row.id for row in conn.execute(select(table.c.id).order_by(table.c.id))]


def test_connect_read_commited(engine_testaccount, assert_text_in_buf):
    metadata = MetaData()
    table_name = "test_connect_read_commited"

    test_table_1 = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by=["id", text("id > 5")],
    )

    metadata.create_all(engine_testaccount)
    try:
        with engine_testaccount.connect().execution_options(
            isolation_level="READ COMMITTED"
        ) as connection:
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), result
            ins = test_table_1.insert().values(id=1, name="test")
            connection.execute(ins)
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] != (None,), (
                "AUTOCOMMIT DISABLED, transaction should be started"
            )

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 0, results  # No insert commited
            assert_text_in_buf("ROLLBACK", occurrences=1)
    finally:
        metadata.drop_all(engine_testaccount)


def test_begin_read_committed(engine_testaccount, assert_text_in_buf):
    metadata = MetaData()
    table_name = "test_begin_read_rc"  # name must not contain "COMMIT" — assert_text_in_buf uses substring match

    test_table_1 = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by=["id", text("id > 5")],
    )

    metadata.create_all(engine_testaccount)
    try:
        assert_text_in_buf("CREATE TABLE", occurrences=1)

        with (
            engine_testaccount.connect().execution_options(
                isolation_level="READ COMMITTED"
            ) as connection,
            connection.begin(),
        ):
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), result
            ins = test_table_1.insert().values(id=1, name="test")
            connection.execute(ins)
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] != (None,), (
                "AUTOCOMMIT DISABLED, transaction should be started"
            )

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 1, results  # Insert commited
            assert_text_in_buf("COMMIT", occurrences=1)
    finally:
        metadata.drop_all(engine_testaccount)


def test_connect_autocommit(engine_testaccount, assert_text_in_buf):
    metadata = MetaData()
    table_name = "test_connect_autocommit"

    test_table_1 = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by=["id", text("id > 5")],
    )

    metadata.create_all(engine_testaccount)
    try:
        with engine_testaccount.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), result
            ins = test_table_1.insert().values(id=1, name="test")
            connection.execute(ins)
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), (
                "Autocommit enabled, transaction should not be started"
            )

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 1, results
            assert_text_in_buf(
                "ROLLBACK using DBAPI connection.rollback()",
                occurrences=1,
            )

    finally:
        metadata.drop_all(engine_testaccount)


def test_begin_autocommit(engine_testaccount, assert_text_in_buf):
    metadata = MetaData()
    table_name = "test_begin_autocommit"

    test_table_1 = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by=["id", text("id > 5")],
    )

    metadata.create_all(engine_testaccount)
    try:
        with (
            engine_testaccount.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection,
            connection.begin(),
        ):
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), result
            ins = test_table_1.insert().values(id=1, name="test")
            connection.execute(ins)
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), (
                "Autocommit enabled, transaction should not be started"
            )

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 1, results

            assert_text_in_buf(
                "COMMIT using DBAPI connection.commit()",
                occurrences=1,
            )

    finally:
        metadata.drop_all(engine_testaccount)


def test_commit_then_rollback_on_same_connection(engine_testaccount, txn_table):
    # SQLAlchemy 2.0 port of the former test_core.py::test_transaction (skipped
    # since 2017 as "No transaction works yet in the core API").
    with engine_testaccount.connect() as conn:
        with conn.begin():
            conn.execute(txn_table.insert().values(id=123, name="committed"))
        trans = conn.begin()
        conn.execute(txn_table.insert().values(id=456, name="rolled back"))
        trans.rollback()

    assert _ids(engine_testaccount, txn_table) == [123]


def test_begin_rolls_back_on_exception(engine_testaccount, txn_table):
    with pytest.raises(ValueError, match="boom"):
        with engine_testaccount.begin() as conn:
            conn.execute(txn_table.insert().values(id=1, name="test"))
            raise ValueError("boom")

    assert _ids(engine_testaccount, txn_table) == []


def test_uncommitted_changes_not_visible_to_other_connections(
    engine_testaccount, txn_table
):
    with engine_testaccount.connect() as writer:
        writer.execute(txn_table.insert().values(id=1, name="test"))
        assert _ids(engine_testaccount, txn_table) == []
        writer.commit()

    assert _ids(engine_testaccount, txn_table) == [1]


def test_ddl_commits_open_transaction(engine_testaccount, txn_table, db_table_name):
    # Snowflake runs each DDL statement in its own transaction and commits any
    # open one first, so DML issued before the DDL can no longer be rolled back.
    other_table = f"{db_table_name}_ddl"
    try:
        with engine_testaccount.connect() as conn:
            conn.execute(txn_table.insert().values(id=1, name="before ddl"))
            assert conn.execute(CURRENT_TRANSACTION).scalar() is not None
            conn.execute(text(f"CREATE TABLE {other_table} (id INT)"))
            assert conn.execute(CURRENT_TRANSACTION).scalar() is None
            conn.rollback()

        assert _ids(engine_testaccount, txn_table) == [1]
    finally:
        with engine_testaccount.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {other_table}"))


def test_begin_nested_is_rejected(engine_testaccount, txn_table):
    # Snowflake has no SAVEPOINT; the error comes from the server and leaves
    # the outer transaction usable.
    with engine_testaccount.connect() as conn:
        with conn.begin():
            conn.execute(txn_table.insert().values(id=1, name="outer"))
            with pytest.raises(ProgrammingError, match="SAVEPOINT"):
                conn.begin_nested()
            assert conn.in_transaction()

    assert _ids(engine_testaccount, txn_table) == [1]


@pytest.mark.parametrize("finish, expected_ids", [("rollback", []), ("commit", [1])])
def test_failed_statement_keeps_transaction_open(
    engine_testaccount, txn_table, finish, expected_ids
):
    # A failing statement does not abort a Snowflake transaction; the rows
    # written before it are kept or discarded by the caller's commit/rollback.
    with engine_testaccount.connect() as conn:
        conn.execute(txn_table.insert().values(id=1, name="ok"))
        with pytest.raises(ProgrammingError):
            conn.execute(text(f"INSERT INTO {txn_table.name} (missing) VALUES (2)"))
        assert conn.execute(CURRENT_TRANSACTION).scalar() is not None
        getattr(conn, finish)()

    assert _ids(engine_testaccount, txn_table) == expected_ids
