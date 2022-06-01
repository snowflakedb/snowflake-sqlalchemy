#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from sqlalchemy import Column, Integer, MetaData, Sequence, String, Table, select


def test_table_with_sequence(engine_testaccount, db_parameters):
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    metadata = MetaData()
    test_table_name = 'sequence'
    sequence_table = Table(test_table_name, metadata,
                           Column('id', Integer, autoincrement=Sequence(test_table_name + '_id_seq'), primary_key=True),
                           Column('data', String(39))
                           )
    sequence_table.create(engine_testaccount)
    seq = Sequence(test_table_name + '_id_seq')
    try:
        engine_testaccount.execute(sequence_table.insert(), [{'data': 'test_insert_1'}])

        select_stmt = select([sequence_table])
        result = engine_testaccount.execute(select_stmt).fetchall()
        assert result == [(1, 'test_insert_1')]

        metadata_autoload = MetaData()
        autoload_sequence_table = Table(test_table_name, metadata_autoload, autoload=True, autoload_with=engine_testaccount)

        engine_testaccount.execute(autoload_sequence_table.insert(),
                                   [{'data': 'multi_insert_1'}, {'data': 'multi_insert_2'}])

        engine_testaccount.execute(autoload_sequence_table.insert(), [{'data': 'test_insert_2'}])

        nextid = engine_testaccount.execute(seq)
        engine_testaccount.execute(autoload_sequence_table.insert(), [{'id': nextid, 'data': 'test_insert_seq'}])
        result = engine_testaccount.execute(select_stmt).fetchall()
        assert result == [
            (1, 'test_insert_1'),
            (2, 'multi_insert_1'),
            (3, 'multi_insert_2'),
            (4, 'test_insert_2'),
            (5, 'test_insert_seq')
        ]
    finally:
        sequence_table.drop(engine_testaccount)
        seq.drop(engine_testaccount)
    return sequence_table


def test_table_with_autoincrement(engine_testaccount, db_parameters):
    # https://github.com/snowflakedb/snowflake-sqlalchemy/issues/124
    metadata = MetaData()
    test_table_name = 'sequence'
    autoincrement_table = Table(test_table_name, metadata,
                                Column('id', Integer, autoincrement=True, primary_key=True),
                                Column('data', String(39))
                                )
    autoincrement_table.create(engine_testaccount)
    try:
        engine_testaccount.execute(autoincrement_table.insert(), [{'data': 'test_insert_1'}])

        select_stmt = select([autoincrement_table])
        result = engine_testaccount.execute(select_stmt).fetchall()
        assert result == [(1, 'test_insert_1')]

        metadata_autoload = MetaData()
        autoload_sequence_table = Table(test_table_name, metadata_autoload, autoload=True, autoload_with=engine_testaccount)

        engine_testaccount.execute(autoload_sequence_table.insert(),
                                   [{'data': 'multi_insert_1'}, {'data': 'multi_insert_2'}])

        engine_testaccount.execute(autoload_sequence_table.insert(), [{'data': 'test_insert_2'}])

        result = engine_testaccount.execute(select_stmt).fetchall()
        assert result == [
            (1, 'test_insert_1'),
            (2, 'multi_insert_1'),
            (3, 'multi_insert_2'),
            (4, 'test_insert_2'),
        ]
    finally:
        autoincrement_table.drop(engine_testaccount)
    return autoincrement_table
