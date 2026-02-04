#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import re

from sqlalchemy import util as sa_util
from sqlalchemy.sql import text

from snowflake.sqlalchemy.name_utils import _NameUtils
from snowflake.sqlalchemy.parser.custom_type_parser import NullType, parse_type


class _StructuredTypeInfoManager:
    """
    Manager for handling structured type information in Snowflake tables.
    This class is responsible for retrieving, caching, and providing
    column information for structured types in Snowflake tables. It maintains
    a cache of column descriptions to avoid repeated database queries.
    Attributes:
        connection: The database connection to use for queries
        full_columns_descriptions (dict): Cache of column descriptions by schema and table
        name_utils (_NameUtils): Utility for normalizing and denormalizing names
        default_schema (str): The default schema to use when none is specified
    """

    def __init__(self, connection, name_utils: _NameUtils, default_schema: str):
        self.connection = connection
        self.full_columns_descriptions = {}
        self.name_utils = name_utils
        self.default_schema = default_schema

    def get_column_info(
        self, schema_name: str, table_name: str, column_name: str, **kwargs
    ):
        self._load_structured_type_info(schema_name, table_name)
        if (
            (schema_name, table_name) in self.full_columns_descriptions
            and column_name in self.full_columns_descriptions[(schema_name, table_name)]
        ):
            return self.full_columns_descriptions[(schema_name, table_name)][
                column_name
            ]
        return None

    def _load_structured_type_info(self, schema_name: str, table_name: str):
        """Get column information for a structured type"""
        if (schema_name, table_name) not in self.full_columns_descriptions:

            column_definitions = self.get_table_columns(table_name, schema_name)
            if not column_definitions:
                self.full_columns_descriptions[(schema_name, table_name)] = {}
                return False

            self.full_columns_descriptions[(schema_name, table_name)] = (
                self._table_columns_as_dict(column_definitions)
            )
        return True

    def _table_columns_as_dict(self, columns: list):
        result = {}
        for column in columns:
            result[column["name"]] = column
        return result

    def get_table_columns(self, table_name: str, schema: str = None):
        """Get all columns in a table in a schema"""
        ans = []

        schema = schema if schema else self.default_schema

        table_schema = self.name_utils.denormalize_name(schema)
        table_name = self.name_utils.denormalize_name(table_name)
        result = self._execute_desc(table_schema, table_name)
        if not result:
            return []

        for desc_data in result:
            column_name = desc_data[0]
            coltype = desc_data[1]
            is_nullable = desc_data[3]
            column_default = desc_data[4]
            primary_key = desc_data[5]
            comment = desc_data[9]

            column_name = self.name_utils.normalize_name(column_name)
            if column_name.startswith("sys_clustering_column"):
                continue  # ignoring clustering column
            type_instance = parse_type(coltype)
            if isinstance(type_instance, NullType):
                sa_util.warn(
                    f"Did not recognize type '{coltype}' of column '{column_name}'"
                )

            identity = None
            match = re.match(
                r"IDENTITY START (?P<start>\d+) INCREMENT (?P<increment>\d+) (?P<order_type>ORDER|NOORDER)",
                column_default if column_default else "",
            )
            if match:
                identity = {
                    "start": int(match.group("start")),
                    "increment": int(match.group("increment")),
                    "order_type": match.group("order_type"),
                }
            is_identity = identity is not None

            ans.append(
                {
                    "name": column_name,
                    "type": type_instance,
                    "nullable": is_nullable == "Y",
                    "default": None if is_identity else column_default,
                    "autoincrement": is_identity,
                    "comment": comment if comment != "" else None,
                    "primary_key": primary_key == "Y",
                }
            )

            if is_identity:
                ans[-1]["identity"] = identity

        # If we didn't find any columns for the table, the table doesn't exist.
        if len(ans) == 0:
            return []
        return ans

    def _execute_desc(self, table_schema: str, table_name: str):
        """Execute a DESC command handling a possible exception.
        Exception can be caused by another session dropping the table while
        once this process has started"""
        try:
            return self.connection.execute(
                text(
                    "DESC /* sqlalchemy:_get_schema_columns */"
                    f" TABLE {table_schema}.{table_name} TYPE = COLUMNS"
                )
            )
        except Exception:
            sa_util.warn(
                f"Failed to reflect '{table_schema}' .'{table_name}' table using sqlalchemy:_get_schema_columns"
            )
        return None
