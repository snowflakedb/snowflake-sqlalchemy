#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

import re
import warnings
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import ReflectedColumn, ReflectedIdentity

from snowflake.sqlalchemy.name_utils import _NameUtils
from snowflake.sqlalchemy.parser.custom_type_parser import NullType, parse_type
from sqlalchemy import text
from sqlalchemy.engine import Connection, CursorResult
from sqlalchemy.exc import ProgrammingError, SAWarning


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

    def __init__(
        self,
        connection: Connection,
        name_utils: _NameUtils,
        default_schema: str,
    ) -> None:
        self.connection = connection
        self.full_columns_descriptions: dict[
            tuple[str, str], dict[str, ReflectedColumn]
        ] = {}
        self.name_utils = name_utils
        self.default_schema = default_schema

    def get_column_info(
        self, schema_name: str, table_name: str, column_name: str, **kwargs: Any
    ) -> ReflectedColumn | None:
        self._load_structured_type_info(schema_name, table_name)
        if (
            (schema_name, table_name) in self.full_columns_descriptions
            and column_name in self.full_columns_descriptions[(schema_name, table_name)]
        ):
            return self.full_columns_descriptions[(schema_name, table_name)][
                column_name
            ]
        return None

    def _load_structured_type_info(self, schema_name: str, table_name: str) -> bool:
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

    def _table_columns_as_dict(
        self, columns: list[ReflectedColumn]
    ) -> dict[str, ReflectedColumn]:
        result: dict[str, ReflectedColumn] = {}
        for column in columns:
            result[column["name"]] = column
        return result

    def get_table_columns_by_full_name(
        self, full_table_name: str
    ) -> list[ReflectedColumn]:
        """
        Get all columns in a table using a fully-qualified table name.

        Args:
            full_table_name: Fully-qualified table name with proper quoting (e.g., "schema"."table")

        Returns:
            List of column information dictionaries
        """
        result = self._execute_desc(full_table_name)
        if not result:
            return []

        return self._parse_desc_result(result)

    def get_table_columns(
        self, table_name: str, schema: str | None = None
    ) -> list[ReflectedColumn]:
        """Get all columns in a table in a schema"""
        schema = schema if schema else self.default_schema

        if "." in str(table_name):
            ip = self.name_utils.identifier_preparer
            table_name = ip._split_schema_by_dot(str(table_name))[-1]

        return self.get_table_columns_by_full_name(
            self.name_utils.always_quote_join(schema, table_name)
        )

    def _parse_desc_result(self, result: CursorResult) -> list[ReflectedColumn]:
        """Parse DESC TABLE result into column information"""
        ans: list[ReflectedColumn] = []

        for desc_data in result:
            column_name = desc_data[0]
            coltype = desc_data[1]
            is_nullable = desc_data[3]
            column_default = desc_data[4]
            primary_key = desc_data[5]
            comment = desc_data[9]

            column_name = self.name_utils.normalize_name(column_name)
            assert column_name is not None  # DESC TABLE always returns a column name
            if column_name.startswith("sys_clustering_column"):
                continue  # ignoring clustering column
            type_instance = parse_type(coltype)
            if isinstance(type_instance, NullType):
                warnings.warn(
                    f"Did not recognize type '{coltype}' of column '{column_name}'",
                    SAWarning,
                    stacklevel=2,
                )

            identity = None
            match = re.match(
                r"IDENTITY START (?P<start>\d+) INCREMENT (?P<increment>\d+) (?P<order_type>ORDER|NOORDER)",
                column_default if column_default else "",
            )
            if match:
                # Build complete identity metadata for SQLAlchemy 2.0+ ReflectedIdentity convention
                identity = cast(
                    "ReflectedIdentity",
                    {
                        "start": int(match.group("start")),
                        "increment": int(match.group("increment")),
                        # Snowflake-specific defaults (same as main reflection path)
                        "always": False,  # Snowflake only supports BY DEFAULT
                        "on_null": None,  # Not separately tracked
                        "cycle": False,  # Snowflake only supports NO CYCLE
                        "order": match.group("order_type") == "ORDER",
                        # Not available via DESC TABLE
                        "minvalue": None,
                        "maxvalue": None,
                        "nominvalue": None,
                        "nomaxvalue": None,
                        "cache": None,
                    },
                )
            is_identity = identity is not None

            ans.append(
                cast(
                    "ReflectedColumn",
                    {
                        "name": column_name,
                        "type": type_instance,
                        "nullable": is_nullable == "Y",
                        "default": None if is_identity else column_default,
                        "autoincrement": is_identity,
                        "comment": comment if comment != "" else None,
                        "primary_key": primary_key == "Y",
                    },
                )
            )

            if is_identity:
                assert identity is not None
                ans[-1]["identity"] = identity

        # If we didn't find any columns for the table, the table doesn't exist.
        if len(ans) == 0:
            return []
        return ans

    def _execute_desc(self, full_table_name: str) -> CursorResult | None:
        """
        Execute a DESC TABLE command handling possible exceptions.

        Args:
            full_table_name: Fully-qualified table name (e.g., schema.table or "schema"."table")

        Returns:
            Query result or None if the command fails

        Note:
            Only SQL-level errors (ProgrammingError) are swallowed — e.g. the
            table was dropped by another session or the object type does not
            support DESC.  Connection / operational errors propagate so callers
            fail fast with actionable diagnostics.
        """
        try:
            return self.connection.execute(
                text(
                    f"DESC /* sqlalchemy:_get_schema_columns */ TABLE {full_table_name} TYPE = COLUMNS"
                )
            )
        except ProgrammingError:
            warnings.warn(
                f"Failed to reflect table '{full_table_name}' using sqlalchemy:_get_schema_columns",
                SAWarning,
                stacklevel=2,
            )
        return None
