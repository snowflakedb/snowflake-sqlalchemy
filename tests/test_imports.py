#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import importlib
import inspect

import pytest


def get_classes_from_module(module_name):
    """Returns a set of class names from a given module."""
    try:
        module = importlib.import_module(module_name)
        members = inspect.getmembers(module)
        return {name for name, obj in members if inspect.isclass(obj)}

    except ImportError:
        print(f"Module '{module_name}' could not be imported.")
        return set()


def test_types_in_snowdialect():
    classes_a = get_classes_from_module(
        "snowflake.sqlalchemy.parser.custom_type_parser"
    )
    classes_b = get_classes_from_module("snowflake.sqlalchemy.snowdialect")
    assert classes_a.issubset(classes_b), str(classes_a - classes_b)


@pytest.mark.parametrize(
    "type_class_name",
    [
        "BIGINT",
        "BINARY",
        "BOOLEAN",
        "CHAR",
        "DATE",
        "DATETIME",
        "DECIMAL",
        "FLOAT",
        "INTEGER",
        "REAL",
        "SMALLINT",
        "TIME",
        "TIMESTAMP",
        "VARCHAR",
        "NullType",
        "_CUSTOM_DECIMAL",
        "ARRAY",
        "DOUBLE",
        "GEOGRAPHY",
        "GEOMETRY",
        "MAP",
        "OBJECT",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
        "VARIANT",
    ],
)
def test_snowflake_data_types_instance(type_class_name):
    classes_b = get_classes_from_module("snowflake.sqlalchemy.snowdialect")
    assert type_class_name in classes_b, type_class_name
