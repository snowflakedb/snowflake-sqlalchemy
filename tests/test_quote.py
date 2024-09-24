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

from sqlalchemy import Column, Integer, MetaData, Sequence, String, Table, inspect


def test_table_name_with_reserved_words(engine_testaccount, db_parameters):
    metadata = MetaData()
    test_table_name = "insert"
    insert_table = Table(
        test_table_name,
        metadata,
        Column("id", Integer, Sequence(f"{test_table_name}_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )

    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_insert = inspector.get_columns(test_table_name)
        assert len(columns_in_insert) == 3
        assert columns_in_insert[0]["autoincrement"] is False
        assert (
            f"{test_table_name}_id_seq.nextval"
            in columns_in_insert[0]["default"].lower()
        )
        assert columns_in_insert[0]["name"] == "id"
        assert columns_in_insert[0]["primary_key"]
        assert not columns_in_insert[0]["nullable"]

        columns_in_insert = inspector.get_columns(
            test_table_name, schema=db_parameters["schema"]
        )
        assert len(columns_in_insert) == 3

    finally:
        insert_table.drop(engine_testaccount)
    return insert_table
