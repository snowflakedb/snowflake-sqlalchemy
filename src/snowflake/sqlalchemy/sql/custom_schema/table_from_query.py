#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import Any

from sqlalchemy.sql import Selectable
from sqlalchemy.sql.schema import Column, MetaData, SchemaItem

from .clustered_table import ClusteredTableBase
from .options.as_query_option import AsQueryOption, AsQueryOptionType
from .options.table_option import TableOptionKey


class TableFromQueryBase(ClusteredTableBase):

    @property
    def as_query(self) -> AsQueryOption | None:
        return self._get_dialect_option(TableOptionKey.AS_QUERY, AsQueryOption)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        as_query: AsQueryOptionType | None = None,
        **kw: Any,
    ) -> None:
        items = [item for item in args]
        created_query = AsQueryOption.create(as_query)
        kw.update(self._as_dialect_options([created_query]))
        if (
            isinstance(created_query, AsQueryOption)
            and isinstance(created_query.query, Selectable)
            and not self.__has_defined_columns(items)
        ):
            columns = self.__create_columns_from_selectable(created_query.query) or []
            args = tuple(items + columns)  # type: ignore[operator]
        super().__init__(name, metadata, *args, **kw)

    def __has_defined_columns(self, items: list[SchemaItem]) -> bool:
        for item in items:
            if isinstance(item, Column):
                return True
        return False

    def __create_columns_from_selectable(
        self, selectable: Selectable
    ) -> list[Column] | None:
        if not isinstance(selectable, Selectable):
            return None
        columns: list[Column] = []
        for _, c in selectable.exported_columns.items():
            columns += [Column(c.name, c.type)]
        return columns
