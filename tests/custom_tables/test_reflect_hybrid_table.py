#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.sql.ddl import CreateTable

from tests.util import random_string


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
@pytest.mark.parametrize(
    "name_case", [str.upper, str.lower], ids=["uppercase", "lowercase"]
)
@pytest.mark.parametrize(
    "explicit_schema", [True, False], ids=["explicit_schema", "default_schema"]
)
def test_reflect_hybrid_table_with_index(
    engine_testaccount, db_parameters, sql_compiler, name_case, explicit_schema
):
    metadata = MetaData()
    # Reflecting through the connection's default schema (schema=None) keys
    # the multi-reflection results differently from an explicit schema — the
    # index must survive both paths.
    schema = db_parameters["schema"] if explicit_schema else None

    table_name = "test_hybrid_table_2_" + random_string(6)
    index_name = name_case("index_name_2")

    create_table_sql = f"""
       CREATE HYBRID TABLE {table_name} (id INT primary key, name VARCHAR, INDEX {index_name} (name));
        """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    table = Table(table_name, metadata, schema=schema, autoload_with=engine_testaccount)

    try:
        assert len(table.indexes) == 1
        # Unquoted identifiers are case-insensitive whichever case they were
        # written in, so reflection returns SQLAlchemy's normalized
        # (lowercase) form.
        assert table.indexes.pop().name == index_name.lower()

    finally:
        metadata.drop_all(engine_testaccount)
