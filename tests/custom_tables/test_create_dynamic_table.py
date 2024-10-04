#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy import Column, Integer, MetaData, String, Table, select

from snowflake.sqlalchemy import DynamicTable
from snowflake.sqlalchemy.sql.custom_schema.options.as_query import AsQuery
from snowflake.sqlalchemy.sql.custom_schema.options.target_lag import (
    TargetLag,
    TimeUnit,
)
from snowflake.sqlalchemy.sql.custom_schema.options.warehouse import Warehouse


def test_create_dynamic_table(engine_testaccount, db_parameters):
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

    dynamic_test_table_1 = DynamicTable(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        TargetLag(1, TimeUnit.HOURS),
        Warehouse(warehouse),
        AsQuery("SELECT id, name from test_table_1;"),
    )

    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            s = select(dynamic_test_table_1)
            results_dynamic_table = conn.execute(s).fetchall()
            s = select(test_table_1)
            results_table = conn.execute(s).fetchall()
            assert results_dynamic_table == results_table

    finally:
        metadata.drop_all(engine_testaccount)


def test_create_dynamic_table_without_dynamictable_class(
    engine_testaccount, db_parameters
):
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

    dynamic_test_table_1 = Table(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        TargetLag(1, TimeUnit.HOURS),
        Warehouse(warehouse),
        AsQuery("SELECT id, name from test_table_1;"),
        AsQuery("SELECT id, name from test_table_1;"),
        prefixes=["DYNAMIC"],
    )

    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            s = select(dynamic_test_table_1)
            results_dynamic_table = conn.execute(s).fetchall()
            s = select(test_table_1)
            results_table = conn.execute(s).fetchall()
            assert results_dynamic_table == results_table

    finally:
        metadata.drop_all(engine_testaccount)
