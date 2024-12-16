#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import Column, Integer, MetaData, String, select, text

from snowflake.sqlalchemy import SnowflakeTable

CURRENT_TRANSACTION = text("SELECT CURRENT_TRANSACTION()")


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
            assert result[0] != (
                None,
            ), "AUTOCOMMIT DISABLED, transaction should be started"

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 0, results  # No insert commited
            assert_text_in_buf("ROLLBACK", occurrences=1)
    finally:
        metadata.drop_all(engine_testaccount)


def test_begin_read_commited(engine_testaccount, assert_text_in_buf):
    metadata = MetaData()
    table_name = "test_begin_read_commited"

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
        ) as connection, connection.begin():
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), result
            ins = test_table_1.insert().values(id=1, name="test")
            connection.execute(ins)
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] != (
                None,
            ), "AUTOCOMMIT DISABLED, transaction should be started"

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 1, results  # Insert commited
            assert_text_in_buf("COMMIT", occurrences=2)
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
            assert result[0] == (
                None,
            ), "Autocommit enabled, transaction should not be started"

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 1, results
            assert_text_in_buf(
                "ROLLBACK using DBAPI connection.rollback(), DBAPI should ignore due to autocommit mode",
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
        with engine_testaccount.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection, connection.begin():
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (None,), result
            ins = test_table_1.insert().values(id=1, name="test")
            connection.execute(ins)
            result = connection.execute(CURRENT_TRANSACTION).fetchall()
            assert result[0] == (
                None,
            ), "Autocommit enabled, transaction should not be started"

        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results = conn.execute(s).fetchall()
            assert len(results) == 1, results
            assert_text_in_buf(
                "COMMIT using DBAPI connection.commit(), DBAPI should ignore due to autocommit mode",
                occurrences=1,
            )

    finally:
        metadata.drop_all(engine_testaccount)
