#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from enum import Enum

from snowflake.sqlalchemy import exc
from snowflake.sqlalchemy.custom_commands import NoneType
from snowflake.sqlalchemy.util import escape_string_literal_interior


class Priority(Enum):
    LOWEST = 0
    VERY_LOW = 1
    LOW = 2
    MEDIUM = 4
    HIGH = 6
    VERY_HIGH = 7
    HIGHEST = 8


class TableOption:
    def __init__(self) -> None:
        self._name: TableOptionKey | None = None

    @property
    def option_name(self) -> str | None:
        if isinstance(self._name, NoneType):
            return None
        return str(self._name.value)

    def _set_option_name(self, name: TableOptionKey | None):
        self._name = name

    @property
    def priority(self) -> Priority:
        return Priority.MEDIUM

    @staticmethod
    def create(**kwargs) -> TableOption:
        raise NotImplementedError

    @staticmethod
    def _get_invalid_table_option(
        parameter_name: TableOptionKey, input_type: str, expected_types: list[str]
    ) -> TableOption:
        from .invalid_table_option import InvalidTableOption

        return InvalidTableOption(
            parameter_name,
            exc.InvalidTableParameterTypeError(
                parameter_name.value, input_type, expected_types
            ),
        )

    def _validate_option(self):
        if isinstance(self.option_name, NoneType):
            raise exc.OptionKeyNotProvidedError(self.__class__.__name__)

    def template(self) -> str:
        name = self.option_name
        assert name is not None, f"option_name not set on {self.__class__.__name__}"
        return f"{name.upper()} = %s"

    @staticmethod
    def _quote_identifier_value(value: str, compiler=None) -> str:
        """Return the identifier quoted per the dialect's rules.

        Uses ``compiler.preparer.quote()`` when a compiler is available so
        that special characters are wrapped in double-quotes and any embedded
        double-quotes are doubled.  Falls back to the bare value when no
        compiler is present (e.g. in ``__repr__`` / test-only paths).
        """
        if compiler is not None:
            return compiler.preparer.quote(value)
        return value

    @staticmethod
    def _escape_string_literal_value(value: str) -> str:
        """Return the escaped interior of a DDL string literal (no surrounding quotes).

        Applies single-quote doubling and backslash doubling for Snowflake's
        ESCAPE_STRING_LITERALS semantics.  Returns only the interior — no
        surrounding quotes — ready to interpolate into a ``'%s'`` template.
        """
        return escape_string_literal_interior(value)

    def render_option(self, compiler) -> str:
        self._validate_option()
        return self._render(compiler)

    def _render(self, compiler) -> str:
        raise NotImplementedError


class TableOptionKey(Enum):
    AS_QUERY = "as_query"
    BASE_LOCATION = "base_location"
    CATALOG = "catalog"
    CATALOG_SYNC = "catalog_sync"
    CLUSTER_BY = "cluster by"
    DATA_RETENTION_TIME_IN_DAYS = "data_retention_time_in_days"
    DEFAULT_DDL_COLLATION = "default_ddl_collation"
    EXTERNAL_VOLUME = "external_volume"
    MAX_DATA_EXTENSION_TIME_IN_DAYS = "max_data_extension_time_in_days"
    REFRESH_MODE = "refresh_mode"
    STORAGE_SERIALIZATION_POLICY = "storage_serialization_policy"
    TARGET_LAG = "target_lag"
    WAREHOUSE = "warehouse"
