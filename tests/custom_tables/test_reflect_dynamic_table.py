#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy import Column, Integer, MetaData, String, Table, select

from snowflake.sqlalchemy import DynamicTable
from snowflake.sqlalchemy.custom_commands import NoneType


def test_simple_reflection_dynamic_table_as_table(engine_testaccount, db_parameters):
    warehouse = db_parameters.get("warehouse", "default")
    metadata = MetaData()
    test_table_1 = Table(
        "test_table_1", metadata, Column("id", Integer), Column("name", String)
    )

    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        ins = test_table_1.insert().values(id=1, name="test")

        conn.execute(ins)
        conn.commit()
    create_table_sql = f"""
   CREATE DYNAMIC TABLE dynamic_test_table (id INT, name VARCHAR)
      TARGET_LAG = '20 minutes'
      WAREHOUSE = {warehouse}
      AS SELECT id, name from test_table_1;
    """
    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    dynamic_test_table = Table(
        "dynamic_test_table", metadata, autoload_with=engine_testaccount
    )

    try:
        with engine_testaccount.connect() as conn:
            s = select(dynamic_test_table)
            results_dynamic_table = conn.execute(s).fetchall()
            s = select(test_table_1)
            results_table = conn.execute(s).fetchall()
            assert results_dynamic_table == results_table

    finally:
        metadata.drop_all(engine_testaccount)


def test_simple_reflection_without_options_loading(engine_testaccount, db_parameters):
    warehouse = db_parameters.get("warehouse", "default")
    metadata = MetaData()
    test_table_1 = Table(
        "test_table_1", metadata, Column("id", Integer), Column("name", String)
    )

    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        ins = test_table_1.insert().values(id=1, name="test")

        conn.execute(ins)
        conn.commit()
    create_table_sql = f"""
   CREATE DYNAMIC TABLE dynamic_test_table (id INT, name VARCHAR)
      TARGET_LAG = '20 minutes'
      WAREHOUSE = {warehouse}
      AS SELECT id, name from test_table_1;
    """
    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    dynamic_test_table = DynamicTable(
        "dynamic_test_table", metadata, autoload_with=engine_testaccount
    )

    # TODO: Add support for loading options when table is reflected
    assert isinstance(dynamic_test_table.warehouse, NoneType)

    try:
        with engine_testaccount.connect() as conn:
            s = select(dynamic_test_table)
            results_dynamic_table = conn.execute(s).fetchall()
            s = select(test_table_1)
            results_table = conn.execute(s).fetchall()
            assert results_dynamic_table == results_table

    finally:
        metadata.drop_all(engine_testaccount)
