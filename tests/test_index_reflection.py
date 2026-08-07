#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import MetaData, inspect
from sqlalchemy.sql.ddl import CreateSchema, DropSchema

from tests.conftest import poll_until
from tests.util import random_string


@pytest.mark.aws
@pytest.mark.parametrize(
    "name_case", [str.upper, str.lower], ids=["uppercase", "lowercase"]
)
@pytest.mark.parametrize(
    "include_columns", [[], ["name3"]], ids=["no_include", "include"]
)
def test_indexes_reflection(
    engine_testaccount, db_parameters, sql_compiler, name_case, include_columns
):
    table_name = "test_hybrid_table_" + random_string(6)
    index_name = name_case("index_name_" + random_string(6))
    schema = db_parameters["schema"]
    index_columns = ["name", "name2"]
    include_sql = f" INCLUDE ({', '.join(include_columns)})" if include_columns else ""

    create_table_sql = f"""
   CREATE HYBRID TABLE {schema}.{table_name} (
        id INT primary key,
        name VARCHAR,
        name2 VARCHAR,
        name3 VARCHAR,
        INDEX {index_name} ({", ".join(index_columns)}){include_sql}
    );
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)
        connection.commit()

    try:
        # On AWS, Hybrid Table (Unistore) index metadata can take a few seconds
        # to propagate before SHOW INDEXES IN TABLE reflects the new index.
        indexes = poll_until(
            lambda: inspect(engine_testaccount).get_indexes(table_name, schema),
            timeout=10,
            interval=0.5,
        )

        assert len(indexes) == 1
        assert indexes[0].get("name") == index_name.lower()
        assert indexes[0].get("column_names") == index_columns
        assert indexes[0].get("include_columns") == include_columns

    finally:
        with engine_testaccount.connect() as connection:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")
            connection.commit()


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

            database = db_parameters["database"].upper()
            assert_text_in_buf(
                f'SHOW /* sqlalchemy:get_schema_tables_info */ TABLES IN SCHEMA "{database}"."{schema.upper()}"',
                occurrences=1,
            )

        finally:
            connection.execute(DropSchema(schema, cascade=True))
