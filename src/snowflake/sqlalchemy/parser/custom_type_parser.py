#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import sqlalchemy.types as sqltypes
from sqlalchemy.types import FLOAT, NullType

from .. import util as sa_util
from ..custom_types import MAP


def parse_column_type(column_type, column_name, dialect):
    index = column_type.find("(")
    type_name = column_type[:index] if index != -1 else column_type
    parameters = column_type[index + 1 : -1] if type_name != column_type else []

    col_type_class = dialect.ischema_names.get(type_name, None)
    col_type_kw = {}
    if col_type_class is None:
        sa_util.warn(
            f"Did not recognize type '{column_type}' of column '{column_name}'"
        )
        col_type_class = NullType
    else:
        if issubclass(col_type_class, FLOAT):
            parameters_list = parameters.split(",")
            col_type_kw["precision"] = int(parameters_list[0]) if parameters else None
            col_type_kw["decimal_return_scale"] = (
                int(parameters_list[1]) if parameters else None
            )
        elif issubclass(col_type_class, sqltypes.Numeric):
            parameters_list = parameters.split(",")
            col_type_kw["precision"] = int(parameters_list[0]) if parameters else None
            col_type_kw["scale"] = int(parameters_list[1]) if parameters else None
        elif issubclass(col_type_class, (sqltypes.String, sqltypes.BINARY)):
            col_type_kw["length"] = int(parameters) if parameters else None
        elif issubclass(col_type_class, MAP):
            parameters_list = parameters.split(", ", maxsplit=1)

            key_type = parameters_list[0]
            value_type = parameters_list[1]
            not_null_str = "NOT NULL"
            not_null = False
            if (
                len(value_type) >= len(not_null_str)
                and value_type[-len(not_null_str) :] == not_null_str
            ):
                not_null = True
                value_type = value_type[: -len(not_null_str) - 1]

            col_type_kw["key_type"] = parse_column_type(
                key_type, column_name + ".key", dialect
            )
            col_type_kw["value_type"] = parse_column_type(
                value_type, column_name + ".value", dialect
            )
            col_type_kw["nullable"] = not_null

    return col_type_class(**col_type_kw)
