#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import typing
from typing import Any, Optional

from sqlalchemy.sql import Selectable
from sqlalchemy.sql.schema import Column, MetaData, SchemaItem

from .clustered_table import ClusteredTableBase
from .options.as_query_option import AsQueryOption, AsQueryOptionType
from .options.table_option import TableOptionKey


class TableFromQueryBase(ClusteredTableBase):

    @property
    def as_query(self) -> Optional[AsQueryOption]:
        return self._get_dialect_option(TableOptionKey.AS_QUERY)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        as_query: AsQueryOptionType = None,
        **kw: Any,
    ) -> None:
        items = [item for item in args]
        as_query = AsQueryOption.create(as_query)  # noqa
        kw.update(self._as_dialect_options([as_query]))
        if (
            isinstance(as_query, AsQueryOption)
            and isinstance(as_query.query, Selectable)
            and not self.__has_defined_columns(items)
        ):
            columns = self.__create_columns_from_selectable(as_query.query)
            args = items + columns
        super().__init__(name, metadata, *args, **kw)

    def __has_defined_columns(self, items: typing.List[SchemaItem]) -> bool:
        for item in items:
            if isinstance(item, Column):
                return True

    def __create_columns_from_selectable(
        self, selectable: Selectable
    ) -> Optional[typing.List[Column]]:
        if not isinstance(selectable, Selectable):
            return
        columns: typing.List[Column] = []
        for _, c in selectable.exported_columns.items():
            columns += [Column(c.name, c.type)]
        return columns
