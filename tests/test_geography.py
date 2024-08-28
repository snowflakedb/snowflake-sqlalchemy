# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from json import loads

from sqlalchemy import Column, Integer, MetaData, Table
from sqlalchemy.sql import select

from snowflake.sqlalchemy import GEOGRAPHY


def test_create_table_geography_datatypes(engine_testaccount):
    """
    Create table including geography data types
    """

    metadata = MetaData()
    table_name = "test_geography0"
    test_geography = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geo", GEOGRAPHY),
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
        Column("id", Integer, primary_key=True),
        Column("geo1", GEOGRAPHY),
        Column("geo2", GEOGRAPHY),
    )
    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            test_point = "POINT(-122.35 37.55)"
            test_point1 = '{"coordinates": [-122.35,37.55],"type": "Point"}'

            ins = test_geography.insert().values(
                id=1, geo1=test_point, geo2=test_point1
            )

            with conn.begin():
                results = conn.execute(ins)
                results.close()

                s = select(test_geography)
                results = conn.execute(s)
                rows = results.fetchone()
                results.close()
                assert rows[0] == 1
                assert rows[1] == rows[2]
                assert loads(rows[2]) == loads(test_point1)
    finally:
        test_geography.drop(engine_testaccount)
