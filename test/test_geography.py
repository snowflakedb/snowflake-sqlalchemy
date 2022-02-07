#!/usr/bin/env python
# -*- coding: utf-8 -*-

from parameters import CONNECTION_PARAMETERS
from snowflake.sqlalchemy import GEOGRAPHY
from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.sql import select
from json import loads

def test_create_table_geography_datatypes(engine_testaccount):
    """
    Create table including geography data types
    """

    metadata = MetaData()
    table_name = "test_geography0"
    test_geography = Table(
        table_name,
        metadata,
        Column('id', Integer, primary_key=True),
        Column('geo', GEOGRAPHY),
        )
    metadata.create_all(engine_testaccount)
    try:
        assert test_geography is not None
    finally:
        test_geography.drop(engine_testaccount)

def test_inspect_geography_datatypes(engine_testaccount):
    """
    Create table including geography data types
    """
    metadata = MetaData()
    table_name = "test_geography0"
    test_geography = Table(
        table_name,
        metadata,
        Column('id', Integer, primary_key=True),
        Column('geo1', GEOGRAPHY),
        Column('geo2', GEOGRAPHY))
    metadata.create_all(engine_testaccount)

    try:
        test_point = 'POINT(-122.35 37.55)'
        test_point1 = '{"coordinates": [-122.35,37.55],"type": "Point"}'

        ins = test_geography.insert().values(
            id=1,
            geo1=test_point,
            geo2=test_point1
        )

        results = engine_testaccount.execute(ins)
        results.close()

        # select
        conn = engine_testaccount.connect()
        s = select([test_geography])
        results = conn.execute(s)
        rows = results.fetchone()
        results.close()
        assert rows[0] == 1
        assert rows[1] == rows[2]
        assert loads(rows[2]) == loads(test_point1)
    finally:
        test_geography.drop(engine_testaccount)

