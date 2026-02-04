#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import typing
from typing import Any, List

from sqlalchemy.sql.schema import MetaData, SchemaItem, Table

from ..._constants import DIALECT_NAME
from ...compat import IS_VERSION_20
from ...custom_commands import NoneType
from ...custom_types import StructuredType
from ...exc import (
    MultipleErrors,
    NoPrimaryKeyError,
    RequiredParametersNotProvidedError,
    StructuredTypeNotSupportedInTableColumnsError,
    UnsupportedPrimaryKeysAndForeignKeysError,
)
from .custom_table_prefix import CustomTablePrefix
from .options.invalid_table_option import InvalidTableOption
from .options.table_option import TableOption, TableOptionKey


class CustomTableBase(Table):
    __table_prefixes__: typing.List[CustomTablePrefix] = []
    _support_primary_and_foreign_keys: bool = True
    _enforce_primary_keys: bool = False
    _required_parameters: List[TableOptionKey] = []
    _support_structured_types: bool = False

    @property
    def table_prefixes(self) -> typing.List[str]:
        return [prefix.name for prefix in self.__table_prefixes__]

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        **kw: Any,
    ) -> None:
        if len(self.__table_prefixes__) > 0:
            prefixes = kw.get("prefixes", []) + self.table_prefixes
            kw.update(prefixes=prefixes)

        if not IS_VERSION_20 and hasattr(super(), "_init"):
            kw.pop("_no_init", True)
            super()._init(name, metadata, *args, **kw)
        else:
            super().__init__(name, metadata, *args, **kw)

        if not kw.get("autoload_with", False):
            self._validate_table()

    def _validate_table(self):
        exceptions: List[Exception] = []

        columns_validation = self.__validate_columns()
        if columns_validation is not None:
            exceptions.append(columns_validation)

        for _, option in self.dialect_options[DIALECT_NAME].items():
            if isinstance(option, InvalidTableOption):
                exceptions.append(option.exception)

        if isinstance(self.key, NoneType) and self._enforce_primary_keys:
            exceptions.append(NoPrimaryKeyError(self.__class__.__name__))
        missing_parameters: List[str] = []

        for required_parameter in self._required_parameters:
            if isinstance(self._get_dialect_option(required_parameter), NoneType):
                missing_parameters.append(required_parameter.name.lower())
        if missing_parameters:
            exceptions.append(
                RequiredParametersNotProvidedError(
                    self.__class__.__name__, missing_parameters
                )
            )

        if not self._support_primary_and_foreign_keys and (
            self.primary_key or self.foreign_keys
        ):
            exceptions.append(
                UnsupportedPrimaryKeysAndForeignKeysError(self.__class__.__name__)
            )

        if len(exceptions) > 1:
            exceptions.sort(key=lambda e: str(e))
            raise MultipleErrors(exceptions)
        elif len(exceptions) == 1:
            raise exceptions[0]

    def __validate_columns(self):
        for column in self.columns:
            if not self._support_structured_types and isinstance(
                column.type, StructuredType
            ):
                return StructuredTypeNotSupportedInTableColumnsError(
                    self.__class__.__name__, self.name, column.name
                )

    def _get_dialect_option(
        self, option_name: TableOptionKey
    ) -> typing.Optional[TableOption]:
        if option_name.value in self.dialect_options[DIALECT_NAME]:
            return self.dialect_options[DIALECT_NAME][option_name.value]
        return None

    def _as_dialect_options(
        self, table_options: List[TableOption]
    ) -> typing.Dict[str, TableOption]:
        result = {}
        for table_option in table_options:
            if isinstance(table_option, TableOption) and isinstance(
                table_option.option_name, str
            ):
                result[DIALECT_NAME + "_" + table_option.option_name] = table_option
        return result

    @classmethod
    def is_equal_type(cls, table: Table) -> bool:
        for prefix in cls.__table_prefixes__:
            if prefix.name not in table._prefixes:
                return False

        return True
