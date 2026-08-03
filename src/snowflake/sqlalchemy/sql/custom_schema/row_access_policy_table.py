#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import Any

from sqlalchemy.sql.schema import MetaData, SchemaItem

from .clustered_table import ClusteredTableBase
from .options.row_access_policy_option import (
    RowAccessPolicyOption,
    RowAccessPolicyOptionType,
)
from .options.table_option import TableOptionKey


class RowAccessPolicyTableBase(ClusteredTableBase):
    @property
    def row_access_policy(self) -> RowAccessPolicyOption | None:
        return self._get_dialect_option(
            TableOptionKey.ROW_ACCESS_POLICY, RowAccessPolicyOption
        )

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args: SchemaItem,
        row_access_policy: RowAccessPolicyOptionType | None = None,
        **kw: Any,
    ) -> None:
        if kw.get("_no_init", True):
            return

        options = [
            RowAccessPolicyOption.create(
                TableOptionKey.ROW_ACCESS_POLICY, row_access_policy
            ),
        ]

        kw.update(self._as_dialect_options(options))
        super().__init__(name, metadata, *args, **kw)
