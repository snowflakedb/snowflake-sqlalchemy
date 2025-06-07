#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from unittest.mock import patch

from sqlalchemy import MetaData, NullPool, Table, create_engine
from sqlalchemy.sql.ddl import CreateTable

from ..snowdialect_mock_utils import MockDBAPI, make_mock_query_map


def mock_dbapi():
    current_schema = [
        ("SQLALCHEMY2", "SQLALCHEMY_SCHEMA_DEMO"),
    ]
    schema_columns_rows = [
        (
            "TEST_SNOWFLAKE_TABLE_REFLECTION",
            "ID",
            "NUMBER",
            None,
            38,
            0,
            "NO",
            None,
            "NO",
            None,
            None,
            None,
        ),
        (
            "TEST_SNOWFLAKE_TABLE_REFLECTION",
            "NAME",
            "TEXT",
            16777216,
            None,
            None,
            "YES",
            None,
            "NO",
            None,
            None,
            None,
        ),
    ]

    tables_info_rows = [
        (
            "2025-05-26 21:51:45.674 -0700",
            "TEST_SNOWFLAKE_TABLE_REFLECTION",
            "SQLALCHEMY2",
            "SQLALCHEMY_TESTS_62B40394_EEB5_44E5_8AF2_378E096F71E8",
            "TABLE",
            None,
            None,
            0,
            0,
            "ACCOUNTADMIN",
            1,
            "OFF",
            "OFF",
            "N",
            "N",
            "ROLE",
            "N",
            "N",
            "N",
            "N",
            "N",
        )
    ]

    primary_keys_rows = [
        (
            "2025-05-26 21:51:45.674 -0700",
            "SQLALCHEMY2",
            "SQLALCHEMY_TESTS_62B40394_EEB5_44E5_8AF2_378E096F71E8",
            "TEST_SNOWFLAKE_TABLE_REFLECTION",
            "ID",
            1,
            "SYS_CONSTRAINT_35753e84-da9a-469c-846e-3124f9d4340f",
            False,
            None,
        )
    ]

    mock_query_map = make_mock_query_map(
        current_schema, schema_columns_rows, tables_info_rows, primary_keys_rows
    )
    return MockDBAPI(mock_query_map)


def test_simple_reflection_hybrid_table_as_table(
    engine_url, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_hybrid_table_reflection"

    with patch("snowflake.connector", mock_dbapi()):
        engine_params = {
            "poolclass": NullPool,
            "future": True,
            "echo": True,
        }
        engine_testaccount = create_engine(engine_url, **engine_params)

        hybrid_test_table = Table(
            table_name, metadata, autoload_with=engine_testaccount
        )

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
