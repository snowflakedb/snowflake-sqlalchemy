#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.sql.ddl import CreateTable


@pytest.mark.aws
def test_simple_reflection_hybrid_table_as_table(
    engine_testaccount, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_hybrid_table_reflection"

    create_table_sql = f"""
   CREATE HYBRID TABLE {table_name} (id INT primary key, name VARCHAR, INDEX index_name (name));
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    hybrid_test_table = Table(table_name, metadata, autoload_with=engine_testaccount)

    constraint = hybrid_test_table.constraints.pop()
    constraint.name = "demo_name"
    hybrid_test_table.constraints.add(constraint)

    try:
        with engine_testaccount.connect():
            value = CreateTable(hybrid_test_table)

            actual = sql_compiler(value)

            # Prefixes reflection not supported, example: "HYBRID, DYNAMIC"
            assert actual == snapshot

    finally:
        metadata.drop_all(engine_testaccount)


@pytest.mark.aws
def test_reflect_hybrid_table_with_index(
    engine_testaccount, db_parameters, sql_compiler
):
    metadata = MetaData()
    schema = db_parameters["schema"]

    table_name = "test_hybrid_table_2"
    index_name = "INDEX_NAME_2"

    create_table_sql = f"""
       CREATE HYBRID TABLE {table_name} (id INT primary key, name VARCHAR, INDEX {index_name} (name));
        """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    table = Table(table_name, metadata, schema=schema, autoload_with=engine_testaccount)

    try:
        assert len(table.indexes) == 1 and table.indexes.pop().name == index_name

    finally:
        metadata.drop_all(engine_testaccount)
