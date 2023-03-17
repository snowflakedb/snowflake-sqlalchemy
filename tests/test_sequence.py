#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import Column, Integer, MetaData, Sequence, String, Table, select


def test_table_with_sequence(engine_testaccount, db_parameters):
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    test_table_name = "sequence"
    test_sequence_name = f"{test_table_name}_id_seq"
    sequence_table = Table(
        test_table_name,
        MetaData(),
        Column("id", Integer, Sequence(test_sequence_name), primary_key=True),
        Column("data", String(39)),
    )
    sequence_table.create(engine_testaccount)
    seq = Sequence(test_sequence_name)
    try:
        with engine_testaccount.connect() as conn:
            with conn.begin():
                conn.execute(sequence_table.insert(), [{"data": "test_insert_1"}])
                select_stmt = select(sequence_table).order_by("id")
                result = conn.execute(select_stmt).fetchall()
                assert result == [(1, "test_insert_1")]
                autoload_sequence_table = Table(
                    test_table_name, MetaData(), autoload_with=engine_testaccount
                )
                conn.execute(
                    autoload_sequence_table.insert(),
                    [{"data": "multi_insert_1"}, {"data": "multi_insert_2"}],
                )
                conn.execute(
                    autoload_sequence_table.insert(), [{"data": "test_insert_2"}]
                )
                nextid = conn.execute(seq)
                conn.execute(
                    autoload_sequence_table.insert(),
                    [{"id": nextid, "data": "test_insert_seq"}],
                )
                result = conn.execute(select_stmt).fetchall()
                assert result == [
                    (1, "test_insert_1"),
                    (2, "multi_insert_1"),
                    (3, "multi_insert_2"),
                    (4, "test_insert_2"),
                    (5, "test_insert_seq"),
                ]
    finally:
        sequence_table.drop(engine_testaccount)
        seq.drop(engine_testaccount)


def test_table_with_autoincrement(engine_testaccount, db_parameters):
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    test_table_name = "sequence"
    autoincrement_table = Table(
        test_table_name,
        MetaData(),
        Column("id", Integer, autoincrement=True, primary_key=True),
        Column("data", String(39)),
    )
    autoincrement_table.create(engine_testaccount)
    try:
        with engine_testaccount.connect() as conn:
            with conn.begin():
                conn.execute(autoincrement_table.insert(), [{"data": "test_insert_1"}])
                select_stmt = select(autoincrement_table).order_by("id")
                result = conn.execute(select_stmt).fetchall()
                assert result == [(1, "test_insert_1")]
                autoload_sequence_table = Table(
                    test_table_name, MetaData(), autoload_with=engine_testaccount
                )
                conn.execute(
                    autoload_sequence_table.insert(),
                    [{"data": "multi_insert_1"}, {"data": "multi_insert_2"}],
                )
                conn.execute(
                    autoload_sequence_table.insert(), [{"data": "test_insert_2"}]
                )
                result = conn.execute(select_stmt).fetchall()
                assert result == [
                    (1, "test_insert_1"),
                    (2, "multi_insert_1"),
                    (3, "multi_insert_2"),
                    (4, "test_insert_2"),
                ]
    finally:
        autoincrement_table.drop(engine_testaccount)
