#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum
from typing import List, Optional

from snowflake.sqlalchemy import exc
from snowflake.sqlalchemy.custom_commands import NoneType


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
        self._name: Optional[TableOptionKey] = None

    @property
    def option_name(self) -> str:
        if isinstance(self._name, NoneType):
            return None
        return str(self._name.value)

    def _set_option_name(self, name: Optional["TableOptionKey"]):
        self._name = name

    @property
    def priority(self) -> Priority:
        return Priority.MEDIUM

    @staticmethod
    def create(**kwargs) -> "TableOption":
        raise NotImplementedError

    @staticmethod
    def _get_invalid_table_option(
        parameter_name: "TableOptionKey", input_type: str, expected_types: List[str]
    ) -> "TableOption":
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
        return f"{self.option_name.upper()} = %s"

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
