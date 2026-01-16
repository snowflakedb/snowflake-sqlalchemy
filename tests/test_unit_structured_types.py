#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy.sql.sqltypes import Float, Integer, Text

from snowflake.sqlalchemy import NUMBER
from snowflake.sqlalchemy.custom_types import MAP, TEXT, VECTOR
from src.snowflake.sqlalchemy.parser.custom_type_parser import (
    parse_type,
    tokenize_parameters,
)


def test_compile_map_with_not_null(snapshot):
    user_table = MAP(NUMBER(10, 0), TEXT(), not_null=True)
    assert user_table.compile() == snapshot


def test_extract_parameters():
    example = "a, b(c, d, f), d"
    assert tokenize_parameters(example) == ["a", "b(c, d, f)", "d"]


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
        (
            "OBJECT(a DECIMAL(10, 0) NOT NULL, b DECIMAL(10, 0), c VARCHAR NOT NULL)",
            "OBJECT(a DECIMAL(10, 0) NOT NULL, b DECIMAL(10, 0), c VARCHAR NOT NULL)",
        ),
        ("ARRAY", "ARRAY"),
        (
            "ARRAY(MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
            "ARRAY(MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
        ),
        ("GEOGRAPHY", "GEOGRAPHY"),
        ("GEOMETRY", "GEOMETRY"),
        ("VECTOR(FLOAT, 3)", "VECTOR(FLOAT, 3)"),
        ("VECTOR(INT, 256)", "VECTOR(INT, 256)"),
    ],
)
def test_snowflake_data_types(input_type, expected_type):
    assert parse_type(input_type).compile() == expected_type


def test_vector_type_accepts_string_and_case_insensitive():
    assert repr(VECTOR("FLOAT", 3)) == "VECTOR(FLOAT, 3)"
    assert repr(VECTOR("int", 256)) == "VECTOR(INT, 256)"
    assert repr(VECTOR("int", 5000)) == "VECTOR(INT, 5000)"


def test_vector_type_accepts_sqlalchemy_types():
    assert repr(VECTOR(Integer(), 8)) == "VECTOR(INT, 8)"
    assert repr(VECTOR(Float(), 2)) == "VECTOR(FLOAT, 2)"


def test_vector_invalid_dimension():
    with pytest.raises(ValueError):
        VECTOR("FLOAT", 0)
    with pytest.raises(ValueError):
        VECTOR("FLOAT", -10)
    with pytest.raises(TypeError):
        VECTOR("FLOAT", "10")


def test_vector_rejects_invalid_element_type():
    with pytest.raises(ValueError):
        VECTOR("STRING", 10)
    with pytest.raises(TypeError):
        VECTOR(Text(), 10)
