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

from sqlalchemy import Integer, Sequence, String
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.sql import select


def test_insert_table(engine_testaccount):
    metadata = MetaData()
    users = Table(
        "users",
        metadata,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
    metadata.create_all(engine_testaccount)

    data = [
        {
            "id": 1,
            "name": "testname1",
            "fullname": "fulltestname1",
        },
        {
            "id": 2,
            "name": "testname2",
            "fullname": "fulltestname2",
        },
    ]
    try:
        with engine_testaccount.connect() as conn:
            # using multivalue insert
            with conn.begin():
                conn.execute(users.insert().values(data))
                results = conn.execute(select(users).order_by("id"))
                row = results.fetchone()
                assert row._mapping["name"] == "testname1"

    finally:
        users.drop(engine_testaccount)
