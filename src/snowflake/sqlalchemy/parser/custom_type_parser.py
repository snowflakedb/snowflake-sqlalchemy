#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import sqlalchemy.types as sqltypes
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    VARCHAR,
    NullType,
)

from ..custom_types import (
    _CUSTOM_DECIMAL,
    ARRAY,
    DOUBLE,
    GEOGRAPHY,
    GEOMETRY,
    MAP,
    OBJECT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
)

ischema_names = {
    "BIGINT": BIGINT,
    "BINARY": BINARY,
    # 'BIT': BIT,
    "BOOLEAN": BOOLEAN,
    "CHAR": CHAR,
    "CHARACTER": CHAR,
    "DATE": DATE,
    "DATETIME": DATETIME,
    "DEC": DECIMAL,
    "DECIMAL": DECIMAL,
    "DOUBLE": DOUBLE,
    "FIXED": DECIMAL,
    "FLOAT": FLOAT,  # Snowflake FLOAT datatype doesn't has parameters
    "INT": INTEGER,
    "INTEGER": INTEGER,
    "NUMBER": _CUSTOM_DECIMAL,
    # 'OBJECT': ?
    "REAL": REAL,
    "BYTEINT": SMALLINT,
    "SMALLINT": SMALLINT,
    "STRING": VARCHAR,
    "TEXT": VARCHAR,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
    "TIMESTAMP_TZ": TIMESTAMP_TZ,
    "TIMESTAMP_LTZ": TIMESTAMP_LTZ,
    "TIMESTAMP_NTZ": TIMESTAMP_NTZ,
    "TINYINT": SMALLINT,
    "VARBINARY": BINARY,
    "VARCHAR": VARCHAR,
    "VARIANT": VARIANT,
    "MAP": MAP,
    "OBJECT": OBJECT,
    "ARRAY": ARRAY,
    "GEOGRAPHY": GEOGRAPHY,
    "GEOMETRY": GEOMETRY,
}


def extract_parameters(text: str) -> list:
    """
    Extracts parameters from a comma-separated string, handling parentheses.

    :param text: A string with comma-separated parameters, which may include parentheses.

    :return: A list of parameters as strings.

    :example:
        For input `"a, (b, c), d"`, the output is `['a', '(b, c)', 'd']`.
    """

    output_parameters = []
    parameter = ""
    open_parenthesis = 0
    for c in text:

        if c == "(":
            open_parenthesis += 1
        elif c == ")":
            open_parenthesis -= 1

        if open_parenthesis > 0 or c != ",":
            parameter += c
        elif c == ",":
            output_parameters.append(parameter.strip(" "))
            parameter = ""
    if parameter != "":
        output_parameters.append(parameter.strip(" "))
    return output_parameters


def parse_type(type_text: str) -> TypeEngine:
    """
    Parses a type definition string and returns the corresponding SQLAlchemy type.

    The function handles types with or without parameters, such as `VARCHAR(255)` or `INTEGER`.

    :param type_text: A string representing a SQLAlchemy type, which may include parameters
                       in parentheses (e.g., "VARCHAR(255)" or "DECIMAL(10, 2)").
    :return: An instance of the corresponding SQLAlchemy type class (e.g., `String`, `Integer`),
             or `NullType` if the type is not recognized.

    :example:
        parse_type("VARCHAR(255)")
        String(length=255)
    """
    index = type_text.find("(")
    type_name = type_text[:index] if index != -1 else type_text
    parameters = (
        extract_parameters(type_text[index + 1 : -1]) if type_name != type_text else []
    )

    col_type_class = ischema_names.get(type_name, None)
    col_type_kw = {}
    if col_type_class is None:
        col_type_class = NullType
    else:
        if issubclass(col_type_class, sqltypes.Numeric):
            col_type_kw = __parse_numeric_type_parameters(parameters)
        elif issubclass(col_type_class, (sqltypes.String, sqltypes.BINARY)):
            col_type_kw = __parse_type_with_length_parameters(parameters)
        elif issubclass(col_type_class, MAP):
            col_type_kw = __parse_map_type_parameters(parameters)
        if col_type_kw is None:
            col_type_class = NullType
            col_type_kw = {}

    return col_type_class(**col_type_kw)


def __parse_map_type_parameters(parameters):
    if len(parameters) != 2:
        return None

    key_type_str = parameters[0]
    value_type_str = parameters[1]
    not_null_str = "NOT NULL"
    not_null = False
    if (
        len(value_type_str) >= len(not_null_str)
        and value_type_str[-len(not_null_str) :] == not_null_str
    ):
        not_null = True
        value_type_str = value_type_str[: -len(not_null_str) - 1]

    key_type: TypeEngine = parse_type(key_type_str)
    value_type: TypeEngine = parse_type(value_type_str)
    if isinstance(key_type, NullType) or isinstance(value_type, NullType):
        return None

    return {
        "key_type": key_type,
        "value_type": value_type,
        "not_null": not_null,
    }


def __parse_type_with_length_parameters(parameters):
    return (
        {"length": int(parameters[0])}
        if len(parameters) == 1 and str.isdigit(parameters[0])
        else {}
    )


def __parse_numeric_type_parameters(parameters):
    result = {}
    if len(parameters) >= 1 and str.isdigit(parameters[0]):
        result["precision"] = int(parameters[0])
    if len(parameters) == 2 and str.isdigit(parameters[1]):
        result["scale"] = int(parameters[1])
    return result
