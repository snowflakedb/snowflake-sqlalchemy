#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import typing
from typing import Any

from sqlalchemy import exc, inspection
from sqlalchemy.sql.schema import MetaData, SchemaItem

from snowflake.sqlalchemy.custom_commands import NoneType

from .options.target_lag import TargetLag
from .options.warehouse import Warehouse
from .table_from_query import TableFromQuery


class DynamicTable(TableFromQuery, inspection.Inspectable["DynamicTable"]):
    """
    A class representing a dynamic table with configurable options and settings.

    The `DynamicTable` class allows for the creation and querying of tables with
    specific options, such as `Warehouse` and `TargetLag`.

    While it does not support reflection at this time, it provides a flexible
    interface for creating dynamic tables and management.

    """

    __table_prefix__ = "DYNAMIC"

    _support_primary_and_foreign_keys = False

    @property
    def warehouse(self) -> typing.Optional[Warehouse]:
        return self._get_dialect_option(Warehouse.__option_name__)

    @property
    def target_lag(self) -> typing.Optional[TargetLag]:
        return self._get_dialect_option(TargetLag.__option_name__)

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

    def _validate_table(self):
        missing_attributes = []
        if self.target_lag is NoneType:
            missing_attributes.append("TargetLag")
        if self.warehouse is NoneType:
            missing_attributes.append("Warehouse")
        if self.as_query is NoneType:
            missing_attributes.append("AsQuery")
        if missing_attributes:
            raise exc.ArgumentError(
                "DYNAMIC TABLE must have the following arguments: %s"
                % ", ".join(missing_attributes)
            )
        super()._validate_table()

    def __repr__(self) -> str:
        return "DynamicTable(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.metadata)]
            + [repr(x) for x in self.columns]
            + [repr(self.target_lag)]
            + [f"{k}={repr(getattr(self, k))}" for k in ["schema"]]
        )
