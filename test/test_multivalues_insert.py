#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from sqlalchemy import Integer, Sequence, String
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import select


def test_insert_table(engine_testaccount):
    metadata = MetaData()
    users = Table('users', metadata,
                  Column('id', Integer, Sequence('user_id_seq'),
                         primary_key=True),
                  Column('name', String),
                  Column('fullname', String),
                  )
    metadata.create_all(engine_testaccount)

    data = [{
        'id': 1,
        'name': 'testname1',
        'fullname': 'fulltestname1',
    }, {
        'id': 2,
        'name': 'testname2',
        'fullname': 'fulltestname2',
    }]
    conn = engine_testaccount.connect()
    try:
        # using multivalue insert
        conn.execute(users.insert(data))
        results = conn.execute(select([users]).order_by('id'))
        row = results.fetchone()
        assert row['name'] == 'testname1'

    finally:
        conn.close()
        users.drop(engine_testaccount)
