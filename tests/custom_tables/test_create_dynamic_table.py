#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, select

from snowflake.sqlalchemy import DynamicTable, exc
from snowflake.sqlalchemy.sql.custom_schema.options.as_query_option import AsQueryOption
from snowflake.sqlalchemy.sql.custom_schema.options.identifier_option import (
    IdentifierOption,
)
from snowflake.sqlalchemy.sql.custom_schema.options.keywords import SnowflakeKeyword
from snowflake.sqlalchemy.sql.custom_schema.options.table_option import TableOptionKey
from snowflake.sqlalchemy.sql.custom_schema.options.target_lag_option import (
    TargetLagOption,
    TimeUnit,
)


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
        target_lag=(1, TimeUnit.HOURS),
        warehouse=warehouse,
        as_query="SELECT id, name from test_table_1;",
        refresh_mode=SnowflakeKeyword.FULL,
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
    engine_testaccount, db_parameters, snapshot
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

    Table(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        snowflake_warehouse=warehouse,
        snowflake_as_query="SELECT id, name from test_table_1;",
        prefixes=["DYNAMIC"],
    )

    with pytest.raises(exc.UnexpectedOptionTypeError) as exc_info:
        metadata.create_all(engine_testaccount)
    assert exc_info.value == snapshot


def test_create_dynamic_table_without_dynamictable_and_defined_options(
    engine_testaccount, db_parameters, snapshot
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

    Table(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        snowflake_target_lag=TargetLagOption.create((1, TimeUnit.HOURS)),
        snowflake_warehouse=IdentifierOption.create(
            TableOptionKey.WAREHOUSE, warehouse
        ),
        snowflake_as_query=AsQueryOption.create("SELECT id, name from test_table_1;"),
        prefixes=["DYNAMIC"],
    )

    with pytest.raises(exc.CustomOptionsAreOnlySupportedOnSnowflakeTables) as exc_info:
        metadata.create_all(engine_testaccount)
    assert exc_info.value == snapshot
