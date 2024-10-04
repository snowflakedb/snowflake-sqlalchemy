#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import MetaData
from sqlalchemy.engine import reflection


@pytest.mark.aws
def test_indexes_reflection(engine_testaccount, db_parameters, sql_compiler):
    metadata = MetaData()

    table_name = "test_hybrid_table_2"
    index_name = "INDEX_NAME_2"
    schema = db_parameters["schema"]

    create_table_sql = f"""
   CREATE HYBRID TABLE {table_name} (id INT primary key, name VARCHAR, INDEX {index_name} (name));
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    insp = reflection.Inspector.from_engine(engine_testaccount)

    try:
        with engine_testaccount.connect():
            # Prefixes reflection not supported, example: "HYBRID, DYNAMIC"
            indexes = insp.get_indexes(table_name, schema)
            assert len(indexes) == 1
            assert indexes[0].get("name") == index_name

    finally:
        metadata.drop_all(engine_testaccount)
