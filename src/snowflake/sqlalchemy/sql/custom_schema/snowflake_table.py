#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Any

from sqlalchemy.sql.schema import MetaData, SchemaItem

from .table_from_query import TableFromQueryBase


class SnowflakeTable(TableFromQueryBase):
    """
    A class representing a table in Snowflake with configurable options and settings.

    While it does not support reflection at this time, it provides a flexible
    interface for creating tables and management.

    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-table
    Example usage:

    SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by = ["id", text("name > 5")]
    )

    Example using explict options:

        SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by = ClusterByOption("id", text("name > 5"))
    )

    """

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
        return "SnowflakeTable(%s)" % ", ".join(
            [repr(self.name)]
            + [repr(self.metadata)]
            + [repr(x) for x in self.columns]
            + [repr(self.cluster_by)]
            + [repr(self.as_query)]
            + [f"{k}={repr(getattr(self, k))}" for k in ["schema"]]
        )
