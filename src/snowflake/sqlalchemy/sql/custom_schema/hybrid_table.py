#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Any

from sqlalchemy.sql.schema import MetaData, SchemaItem

from .custom_table_base import CustomTableBase
from .custom_table_prefix import CustomTablePrefix


class HybridTable(CustomTableBase):
    """
    A class representing a hybrid table with configurable options and settings.

    The `HybridTable` class allows for the creation and querying of OLTP Snowflake Tables .

    While it does not support reflection at this time, it provides a flexible
    interface for creating hybrid tables and management.

    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table

    Example usage:
    HybridTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String)
    )
    """

    __table_prefixes__ = [CustomTablePrefix.HYBRID]
    _enforce_primary_keys: bool = True
    _support_structured_types = True

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        **kw: Any,
    ) -> None:
        if kw.get("_no_init", True):
            return
        super().__init__(name, metadata, *args, **kw)

    def _init(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        **kw: Any,
    ) -> None:
        self.__init__(name, metadata, *args, _no_init=False, **kw)

    def __repr__(self) -> str:
        return "HybridTable(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.metadata)]
            + [repr(x) for x in self.columns]
            + [f"{k}={repr(getattr(self, k))}" for k in ["schema"]]
        )
