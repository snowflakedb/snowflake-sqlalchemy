#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from sqlalchemy import (Table, Column, Integer, String, MetaData, Sequence)
from sqlalchemy import inspect


def test_table_name_with_reserved_words(engine_testaccount, db_parameters):
    metadata = MetaData()
    test_table_name = 'insert'
    insert_table = Table(test_table_name, metadata,
                         Column('id', Integer, Sequence(test_table_name + '_id_seq'),
                                primary_key=True),
                         Column('name', String),
                         Column('fullname', String),
                         )

    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_insert = inspector.get_columns(test_table_name)
        assert len(columns_in_insert) == 3
        assert columns_in_insert[0]['autoincrement'], 'autoinrecment'
        assert columns_in_insert[0]['default'] is None, 'default'
        assert columns_in_insert[0]['name'] == 'id', 'name'
        assert columns_in_insert[0]['primary_key'], 'primary key'
        assert not columns_in_insert[0]['nullable']

        columns_in_insert = inspector.get_columns(test_table_name, schema=db_parameters['schema'])
        assert len(columns_in_insert) == 3

    finally:
        insert_table.drop(engine_testaccount)
    return insert_table
