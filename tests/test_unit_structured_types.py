#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.sqlalchemy import NUMBER
from snowflake.sqlalchemy.custom_types import MAP, TEXT
from src.snowflake.sqlalchemy.parser.custom_type_parser import (
    extract_parameters,
    parse_type,
)


def test_compile_map_with_not_null(snapshot):
    user_table = MAP(NUMBER(10, 0), TEXT(), not_null=True)
    assert user_table.compile() == snapshot


def test_extract_parameters():
    example = "a, b(c, d, f), d"
    assert extract_parameters(example) == ["a", "b(c, d, f)", "d"]


@pytest.mark.parametrize(
    "input_type, expected_type",
    [
        ("BIGINT", "BIGINT"),
        ("BINARY(16)", "BINARY(16)"),
        ("BOOLEAN", "BOOLEAN"),
        ("CHAR(5)", "CHAR(5)"),
        ("CHARACTER(5)", "CHAR(5)"),
        ("DATE", "DATE"),
        ("DATETIME(3)", "DATETIME"),
        ("DECIMAL(10, 2)", "DECIMAL(10, 2)"),
        ("DEC(10, 2)", "DECIMAL(10, 2)"),
        ("DOUBLE", "FLOAT"),
        ("FLOAT", "FLOAT"),
        ("FIXED(10, 2)", "DECIMAL(10, 2)"),
        ("INT", "INTEGER"),
        ("INTEGER", "INTEGER"),
        ("NUMBER(12, 4)", "DECIMAL(12, 4)"),
        ("REAL", "REAL"),
        ("BYTEINT", "SMALLINT"),
        ("SMALLINT", "SMALLINT"),
        ("STRING(255)", "VARCHAR(255)"),
        ("TEXT(255)", "VARCHAR(255)"),
        ("VARCHAR(255)", "VARCHAR(255)"),
        ("TIME(6)", "TIME"),
        ("TIMESTAMP(3)", "TIMESTAMP"),
        ("TIMESTAMP_TZ(3)", "TIMESTAMP_TZ"),
        ("TIMESTAMP_LTZ(3)", "TIMESTAMP_LTZ"),
        ("TIMESTAMP_NTZ(3)", "TIMESTAMP_NTZ"),
        ("TINYINT", "SMALLINT"),
        ("VARBINARY(16)", "BINARY(16)"),
        ("VARCHAR(255)", "VARCHAR(255)"),
        ("VARIANT", "VARIANT"),
        (
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
        ),
        (
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR))",
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR))",
        ),
        ("MAP(DECIMAL(10, 0), VARIANT)", "MAP(DECIMAL(10, 0), VARIANT)"),
        ("OBJECT", "OBJECT"),
        ("ARRAY", "ARRAY"),
        ("GEOGRAPHY", "GEOGRAPHY"),
        ("GEOMETRY", "GEOMETRY"),
    ],
)
def test_snowflake_data_types(input_type, expected_type):
    assert parse_type(input_type).compile() == expected_type
