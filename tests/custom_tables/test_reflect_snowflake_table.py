#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy import MetaData, Table, inspect
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import SnowflakeTable


def test_simple_reflection_of_table_as_sqlalchemy_table(
    engine_testaccount, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_snowflake_table_reflection"

    create_table_sql = f"""
   CREATE TABLE {table_name} (id INT primary key, name VARCHAR);
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    snowflake_test_table = Table(table_name, metadata, autoload_with=engine_testaccount)
    constraint = snowflake_test_table.constraints.pop()
    constraint.name = "demo_name"
    snowflake_test_table.constraints.add(constraint)

    try:
        with engine_testaccount.connect():
            value = CreateTable(snowflake_test_table)

            actual = sql_compiler(value)

            assert actual == snapshot

    finally:
        metadata.drop_all(engine_testaccount)


def test_simple_reflection_of_table_as_snowflake_table(
    engine_testaccount, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_snowflake_table_reflection"

    create_table_sql = f"""
   CREATE TABLE {table_name} (id INT primary key, name VARCHAR);
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    snowflake_test_table = SnowflakeTable(
        table_name, metadata, autoload_with=engine_testaccount
    )
    constraint = snowflake_test_table.constraints.pop()
    constraint.name = "demo_name"
    snowflake_test_table.constraints.add(constraint)

    try:
        with engine_testaccount.connect():
            value = CreateTable(snowflake_test_table)

            actual = sql_compiler(value)

            assert actual == snapshot

    finally:
        metadata.drop_all(engine_testaccount)


def test_inspect_snowflake_table(
    engine_testaccount, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_snowflake_table_inspect"

    create_table_sql = f"""
   CREATE TABLE {table_name} (id INT primary key, name VARCHAR);
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    try:
        with engine_testaccount.connect() as conn:
            insp = inspect(conn)
            table = insp.get_columns(table_name)
            assert table == snapshot

    finally:
        metadata.drop_all(engine_testaccount)
