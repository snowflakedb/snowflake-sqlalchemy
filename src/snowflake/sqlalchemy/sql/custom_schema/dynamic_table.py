#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import typing
from typing import Any, Union

from sqlalchemy.sql.schema import MetaData, SchemaItem

from .custom_table_prefix import CustomTablePrefix
from .options import (
    IdentifierOption,
    IdentifierOptionType,
    KeywordOptionType,
    TableOptionKey,
    TargetLagOption,
    TargetLagOptionType,
)
from .options.keyword_option import KeywordOption
from .table_from_query import TableFromQueryBase


class DynamicTable(TableFromQueryBase):
    """
    A class representing a dynamic table with configurable options and settings.

    The `DynamicTable` class allows for the creation and querying of tables with
    specific options, such as `Warehouse` and `TargetLag`.

    While it does not support reflection at this time, it provides a flexible
    interface for creating dynamic tables and management.

    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    Example using option values:
        DynamicTable(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        target_lag=(1, TimeUnit.HOURS),
        warehouse='warehouse_name',
        refresh_mode=SnowflakeKeyword.AUTO
        as_query="SELECT id, name from test_table_1;"
    )

    Example using explicit options:
        DynamicTable(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        target_lag=TargetLag(1, TimeUnit.HOURS),
        warehouse=Identifier('warehouse_name'),
        refresh_mode=KeywordOption(SnowflakeKeyword.AUTO)
        as_query=AsQuery("SELECT id, name from test_table_1;")
    )
    """

    __table_prefixes__ = [CustomTablePrefix.DYNAMIC]
    _support_primary_and_foreign_keys = False
    _required_parameters = [
        TableOptionKey.WAREHOUSE,
        TableOptionKey.AS_QUERY,
        TableOptionKey.TARGET_LAG,
    ]

    @property
    def warehouse(self) -> typing.Optional[IdentifierOption]:
        return self._get_dialect_option(TableOptionKey.WAREHOUSE)

    @property
    def target_lag(self) -> typing.Optional[TargetLagOption]:
        return self._get_dialect_option(TableOptionKey.TARGET_LAG)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        warehouse: IdentifierOptionType = None,
        target_lag: Union[TargetLagOptionType, KeywordOptionType] = None,
        refresh_mode: KeywordOptionType = None,
        **kw: Any,
    ) -> None:
        if kw.get("_no_init", True):
            return

        options = [
            IdentifierOption.create(TableOptionKey.WAREHOUSE, warehouse),
            TargetLagOption.create(target_lag),
            KeywordOption.create(TableOptionKey.REFRESH_MODE, refresh_mode),
        ]

        kw.update(self._as_dialect_options(options))
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
        return "DynamicTable(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.metadata)]
            + [repr(x) for x in self.columns]
            + [repr(self.target_lag)]
            + [repr(self.warehouse)]
            + [repr(self.cluster_by)]
            + [repr(self.as_query)]
            + [f"{k}={repr(getattr(self, k))}" for k in ["schema"]]
        )
