#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import importlib
import inspect


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
    return classes_a.issubset(classes_b), classes_a - classes_b
