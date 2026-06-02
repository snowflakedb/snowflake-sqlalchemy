#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import Any

from sqlalchemy.sql.schema import MetaData, SchemaItem

from .custom_table_base import CustomTableBase
from .options.cluster_by_option import ClusterByOption, ClusterByOptionType
from .options.table_option import TableOptionKey


class ClusteredTableBase(CustomTableBase):

    @property
    def cluster_by(self) -> ClusterByOption | None:
        return self._get_dialect_option(TableOptionKey.CLUSTER_BY)  # type: ignore[return-value]

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        cluster_by: ClusterByOptionType | None = None,
        **kw: Any,
    ) -> None:
        if kw.get("_no_init", True):
            return

        options = [
            ClusterByOption.create(cluster_by),
        ]

        kw.update(self._as_dialect_options(options))
        super().__init__(name, metadata, *args, **kw)
