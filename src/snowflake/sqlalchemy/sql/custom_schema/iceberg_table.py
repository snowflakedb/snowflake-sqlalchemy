#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import typing
from typing import Any

from sqlalchemy.sql.schema import MetaData, SchemaItem

from .custom_table_prefix import CustomTablePrefix
from .options import LiteralOption, LiteralOptionType, TableOptionKey
from .table_from_query import TableFromQueryBase


class IcebergTable(TableFromQueryBase):
    """
    A class representing an iceberg table with configurable options and settings.

    While it does not support reflection at this time, it provides a flexible
    interface for creating iceberg tables and management.

    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table

    Example using option values:

        IcebergTable(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        external_volume='my_external_volume',
        base_location='my_iceberg_table'"
    )

    Example using explicit options:
        DynamicTable(
        "dynamic_test_table_1",
        metadata,
        Column("id", Integer),
        Column("name", String),
        external_volume=LiteralOption('my_external_volume')
        base_location=LiteralOption('my_iceberg_table')
    )
    """

    __table_prefixes__ = [CustomTablePrefix.ICEBERG]
    _support_structured_types = True

    @property
    def external_volume(self) -> typing.Optional[LiteralOption]:
        return self._get_dialect_option(TableOptionKey.EXTERNAL_VOLUME)

    @property
    def base_location(self) -> typing.Optional[LiteralOption]:
        return self._get_dialect_option(TableOptionKey.BASE_LOCATION)

    @property
    def catalog(self) -> typing.Optional[LiteralOption]:
        return self._get_dialect_option(TableOptionKey.CATALOG)

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        external_volume: LiteralOptionType = None,
        base_location: LiteralOptionType = None,
        **kw: Any,
    ) -> None:
        if kw.get("_no_init", True):
            return

        options = [
            LiteralOption.create(TableOptionKey.EXTERNAL_VOLUME, external_volume),
            LiteralOption.create(TableOptionKey.BASE_LOCATION, base_location),
            LiteralOption.create(TableOptionKey.CATALOG, "SNOWFLAKE"),
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
        return "IcebergTable(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.metadata)]
            + [repr(x) for x in self.columns]
            + [repr(self.external_volume)]
            + [repr(self.base_location)]
            + [repr(self.catalog)]
            + [repr(self.cluster_by)]
            + [repr(self.as_query)]
            + [f"{k}={repr(getattr(self, k))}" for k in ["schema"]]
        )
