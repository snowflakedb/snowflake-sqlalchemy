#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import typing
from typing import Any, Optional

import sqlalchemy
from sqlalchemy.sql import Selectable
from sqlalchemy.sql.schema import Column, MetaData, SchemaItem
from sqlalchemy.util import NoneType

from .custom_table_base import CustomTableBase
from .options.as_query import AsQuery


class TableFromQueryBase(CustomTableBase):

    @property
    def as_query(self):
        return self._get_dialect_option(AsQuery.__option_name__)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        **kw: Any,
    ) -> None:
        items = [item for item in args]
        as_query: AsQuery = self.__get_as_query_from_items(items)
        if (
            as_query is not NoneType
            and isinstance(as_query.query, Selectable)
            and not self.__has_defined_columns(items)
        ):
            columns = self.__create_columns_from_selectable(as_query.query)
            args = items + columns
        super().__init__(name, metadata, *args, **kw)

    def __get_as_query_from_items(
        self, items: typing.List[SchemaItem]
    ) -> Optional[AsQuery]:
        for item in items:
            if isinstance(item, AsQuery):
                return item
        return NoneType

    def __has_defined_columns(self, items: typing.List[SchemaItem]) -> bool:
        for item in items:
            if isinstance(item, Column):
                return True

    def __create_columns_from_selectable(
        self, selectable: Selectable
    ) -> Optional[typing.List[Column]]:
        if not isinstance(selectable, sqlalchemy.Selectable):
            return
        columns: typing.List[Column] = []
        for _, c in selectable.exported_columns.items():
            columns += [Column(c.name, c.type)]
        return columns
