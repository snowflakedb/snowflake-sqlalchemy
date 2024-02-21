#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    insert,
    select,
)


def test_table_with_sequence(engine_testaccount):
    """Snowflake does not guarantee generating sequence numbers without gaps.

    The generated numbers are not necesairly contigous.
    https://docs.snowflake.com/en/user-guide/querying-sequences
    """
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    test_table_name = "sequence"
    test_sequence_name = f"{test_table_name}_id_seq"
    metadata = MetaData()

    sequence_table = Table(
        test_table_name,
        metadata,
        Column(
            "id",
            Integer,
            Sequence(test_sequence_name, order=True),
            primary_key=True,
        ),
        Column("data", String(39)),
    )

    metadata.create_all(engine_testaccount)

    sequence_insert_stmt = insert(sequence_table)
    sequence_select_stmt = select(sequence_table.c.data).order_by("id")

    second_metadata = MetaData()
    autoload_sequence_table = Table(
        test_table_name,
        second_metadata,
        autoload_with=engine_testaccount,
    )
    seq = Sequence(test_sequence_name, order=True)

    try:
        with engine_testaccount.connect() as conn:
            conn.execute(sequence_insert_stmt, ({"data": "test_insert_1"}))
            result = conn.execute(sequence_select_stmt).fetchall()
            assert result == [("test_insert_1",)]

            autoload_sequence_table = Table(
                test_table_name,
                second_metadata,
                autoload_with=engine_testaccount,
            )

            conn.execute(
                insert(autoload_sequence_table),
                [
                    {"data": "multi_insert_1"},
                    {"data": "multi_insert_2"},
                ],
            )
            conn.execute(
                insert(autoload_sequence_table),
                [
                    {"data": "test_insert_2"},
                ],
            )
            nextid = conn.execute(seq)
            conn.execute(
                insert(autoload_sequence_table),
                [{"id": nextid, "data": "test_insert_seq"}],
            )
            conn.commit()
            result = conn.execute(sequence_select_stmt).fetchall()
            result = conn.execute(select(sequence_table)).fetchall()
            assert result == [
                (1, "test_insert_1"),
                (2, "multi_insert_1"),
                (3, "multi_insert_2"),
                (4, "test_insert_2"),
                (5, "test_insert_seq"),
            ], result

    finally:
        metadata.drop_all(engine_testaccount)


def test_table_with_autoincrement(engine_testaccount):
    """Snowflake does not guarantee generating sequence numbers without gaps.

    The generated numbers are not necesairly contigous.
    https://docs.snowflake.com/en/user-guide/querying-sequences
    """
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    test_table_name = "sequence"
    metadata = MetaData()
    autoincrement_table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, autoincrement=True, primary_key=True),
        Column("data", String(39)),
    )
    metadata.create_all(engine_testaccount)

    select_stmt = select(autoincrement_table.c.data)

    try:
        with engine_testaccount.connect() as conn:
            conn.execute(insert(autoincrement_table), ({"data": "test_insert_1"}))
            result = conn.execute(select_stmt).fetchall()
            assert result == [("test_insert_1",)], result

            autoload_sequence_table = Table(
                test_table_name,
                MetaData(),
                autoload_with=engine_testaccount,
            )
            conn.execute(
                insert(autoload_sequence_table),
                [
                    {"data": "multi_insert_1"},
                    {"data": "multi_insert_2"},
                ],
            )
            conn.execute(insert(autoload_sequence_table), [{"data": "test_insert_2"}])
            result = conn.execute(select_stmt).fetchall()
            assert result == [
                ("test_insert_1",),
                ("multi_insert_1",),
                ("multi_insert_2",),
                ("test_insert_2",),
            ]
    finally:
        metadata.drop_all(engine_testaccount)
