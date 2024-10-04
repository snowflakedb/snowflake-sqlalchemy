#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Any

from sqlalchemy.exc import ArgumentError
from sqlalchemy.sql.schema import MetaData, SchemaItem

from snowflake.sqlalchemy.custom_commands import NoneType

from .custom_table_base import CustomTableBase


class HybridTable(CustomTableBase):
    """
    A class representing a hybrid table with configurable options and settings.

    The `HybridTable` class allows for the creation and querying of OLTP Snowflake Tables .

    While it does not support reflection at this time, it provides a flexible
    interface for creating dynamic tables and management.
    """

    __table_prefix__ = "HYBRID"

    _support_primary_and_foreign_keys = True

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
        super().__init__(name, metadata, *args, **kw)

    def _validate_table(self):
        missing_attributes = []
        if self.key is NoneType:
            missing_attributes.append("Primary Key")
        if missing_attributes:
            raise ArgumentError(
                "HYBRID TABLE must have the following arguments: %s"
                % ", ".join(missing_attributes)
            )
        super()._validate_table()

    def __repr__(self) -> str:
        return "HybridTable(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.metadata)]
            + [repr(x) for x in self.columns]
            + [f"{k}={repr(getattr(self, k))}" for k in ["schema"]]
        )
