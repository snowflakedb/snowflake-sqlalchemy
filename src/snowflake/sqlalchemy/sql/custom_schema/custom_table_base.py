#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import typing
from typing import Any

from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.schema import MetaData, SchemaItem, Table

from ..._constants import DIALECT_NAME
from ...compat import IS_VERSION_20
from ...custom_commands import NoneType
from .custom_table_prefix import CustomTablePrefix
from .options.table_option import TableOption


class CustomTableBase(Table):
    __table_prefixes__: typing.List[CustomTablePrefix] = []
    _support_primary_and_foreign_keys: bool = True

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
                f"Primary key and foreign keys are not supported in {' '.join(self.table_prefixes)} TABLE."
            )

        return True

    def _get_dialect_option(self, option_name: str) -> typing.Optional[TableOption]:
        if option_name in self.dialect_options[DIALECT_NAME]:
            return self.dialect_options[DIALECT_NAME][option_name]
        return NoneType

    @classmethod
    def is_equal_type(cls, table: Table) -> bool:
        for prefix in cls.__table_prefixes__:
            if prefix.name not in table._prefixes:
                return False

        return True
