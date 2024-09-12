#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import typing
from typing import Any

from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.schema import MetaData, SchemaItem, Table

from ...compat import IS_VERSION_20
from ...constants import DIALECT_NAME
from ...custom_commands import NoneType
from .options.table_option import TableOption


class CustomTableBase(Table):
    __table_prefix__ = ""
    _support_primary_and_foreign_keys = True

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        **kw: Any,
    ) -> None:
        if self.__table_prefix__ != "":
            prefixes = kw.get("prefixes", []) + [self.__table_prefix__]
            kw.update(prefixes=prefixes)
        if not IS_VERSION_20 and hasattr(super(), "_init"):
            super()._init(name, metadata, *args, **kw)
        else:
            super().__init__(name, metadata, *args, **kw)

        if not kw.get("autoload_with", False):
            self._validate_table()

    def _validate_table(self):
        if not self._support_primary_and_foreign_keys and (
            self.primary_key or self.foreign_keys
        ):
            raise ArgumentError(
                f"Primary key and foreign keys are not supported in {self.__table_prefix__} TABLE."
            )

        return True

    def _get_dialect_option(self, option_name: str) -> typing.Optional[TableOption]:
        if option_name in self.dialect_options[DIALECT_NAME]:
            return self.dialect_options[DIALECT_NAME][option_name]
        return NoneType
