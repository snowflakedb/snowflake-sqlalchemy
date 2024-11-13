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

from .. import util as sa_util
from ..custom_types import (
    _CUSTOM_DECIMAL,
    ARRAY,
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
    "DOUBLE": FLOAT,
    "FIXED": DECIMAL,
    "FLOAT": FLOAT,
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


def parse_column_type(column_type: str, column_name: str = "") -> TypeEngine:
    index = column_type.find("(")
    type_name = column_type[:index] if index != -1 else column_type
    parameters = column_type[index + 1 : -1] if type_name != column_type else []

    col_type_class = ischema_names.get(type_name, None)
    col_type_kw = {}
    if col_type_class is None:
        col_type_class = NullType
    else:
        if issubclass(col_type_class, FLOAT) and parameters:
            col_type_kw = _parse_float_type_parameters(parameters)
        elif issubclass(col_type_class, sqltypes.Numeric):
            col_type_kw = _parse_numeric_type_parameters(parameters)
        elif issubclass(col_type_class, (sqltypes.String, sqltypes.BINARY)):
            col_type_kw = _parse_type_with_length_parameters(parameters)
        elif issubclass(col_type_class, MAP):
            col_type_kw = _parse_map_type_parameters(parameters)
        if col_type_kw is None:
            col_type_class = NullType
    if col_type_class is NullType and column_name != "":
        sa_util.warn(
            f"Did not recognize type '{column_type}' of column '{column_name}'"
        )

    return col_type_class(**col_type_kw)


def _parse_map_type_parameters(parameters):
    parameters_list = parameters.split(", ", maxsplit=1)
    if len(parameters_list) != 2:
        return None

    key_type_str = parameters_list[0]
    value_type_str = parameters_list[1]
    not_null_str = "NOT NULL"
    not_null = False
    if (
        len(value_type_str) >= len(not_null_str)
        and value_type_str[-len(not_null_str) :] == not_null_str
    ):
        not_null = True
        value_type_str = value_type_str[: -len(not_null_str) - 1]

    key_type: TypeEngine = parse_column_type(key_type_str)
    value_type: TypeEngine = parse_column_type(value_type_str)
    if isinstance(key_type, NullType) or isinstance(value_type, NullType):
        return None

    return {
        "key_type": key_type,
        "value_type": value_type,
        "nullable": not_null,
    }


def _parse_type_with_length_parameters(parameters):
    return {"length": int(parameters)} if str.isdigit(parameters) else None


def _parse_numeric_type_parameters(parameters):
    parameters_list = parameters.split(",") if parameters else []
    if (
        len(parameters_list) == 2
        and str.isdigit(parameters_list[0])
        and str.isdigit(parameters_list[1])
    ):
        return {
            "precision": int(parameters_list[0]),
            "scale": int(parameters_list[1]),
        }

    return None


def _parse_float_type_parameters(parameters):
    parameters_list = parameters.split(",") if parameters else []
    if (
        len(parameters_list) == 2
        and str.isdigit(parameters_list[0])
        and str.isdigit(parameters_list[1])
    ):
        return {
            "precision": int(parameters_list[0]),
            "decimal_return_scale": int(parameters_list[1]),
        }

    return None
