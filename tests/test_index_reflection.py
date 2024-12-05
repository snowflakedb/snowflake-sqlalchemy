#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import MetaData, inspect
from sqlalchemy.sql.ddl import CreateSchema, DropSchema


@pytest.mark.aws
def test_indexes_reflection(engine_testaccount, db_parameters, sql_compiler):
    metadata = MetaData()

    table_name = "test_hybrid_table_2"
    index_name = "INDEX_NAME_2"
    schema = db_parameters["schema"]
    index_columns = ["name", "name2"]

    create_table_sql = f"""
   CREATE HYBRID TABLE {table_name} (
        id INT primary key,
        name VARCHAR,
        name2 VARCHAR,
        INDEX {index_name} ({', '.join(index_columns)})
    );
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    insp = inspect(engine_testaccount)

    try:
        with engine_testaccount.connect():
            # Prefixes reflection not supported, example: "HYBRID, DYNAMIC"
            indexes = insp.get_indexes(table_name, schema)
            assert len(indexes) == 1
            assert indexes[0].get("name") == index_name
            assert indexes[0].get("column_names") == index_columns

    finally:
        metadata.drop_all(engine_testaccount)


@pytest.mark.aws
def test_simple_reflection_hybrid_table_as_table(
    engine_testaccount, assert_text_in_buf, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_simple_reflection_hybrid_table_as_table"
    schema = db_parameters["schema"] + "_reflections"
    with engine_testaccount.connect() as connection:
        try:
            connection.execute(CreateSchema(schema))

            create_table_sql = f"""
           CREATE HYBRID TABLE {schema}.{table_name} (id INT primary key, new_column VARCHAR, INDEX index_name (new_column));
            """
            connection.exec_driver_sql(create_table_sql)

            metadata.reflect(engine_testaccount, schema=schema)

            assert_text_in_buf(
                f"SHOW /* sqlalchemy:get_schema_tables_info */ TABLES IN SCHEMA {schema}",
                occurrences=1,
            )

        finally:
            connection.execute(DropSchema(schema, cascade=True))
