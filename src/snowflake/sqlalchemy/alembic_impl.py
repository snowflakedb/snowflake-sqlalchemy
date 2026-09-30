#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Alembic implementation for Snowflake.

There are some compatibility issues that SnowflakeImpl solves to ensure
smooth migrations:

- Type synonyms
- Awaiting index builds
- Ignoring hybrid tables' implicit foreign key backing indexes.

This module is automatically loaded via the `sqlalchemy.dialects` entry-point
to automatically register this.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from alembic.ddl.impl import DefaultImpl

from sqlalchemy.schema import CreateIndex

if TYPE_CHECKING:
    from sqlalchemy.sql.schema import (
        Index,
        Table,
        UniqueConstraint,
    )


class SnowflakeImpl(DefaultImpl):
    __dialect__ = "snowflake"
    index_build_timeout: float | None = 600.0
    index_build_poke_interval: float = 5.0

    type_synonyms = DefaultImpl.type_synonyms + (
        {
            "VARCHAR",
            "CHAR",
            "CHARACTER",
            "NCHAR",
            "STRING",
            "TEXT",
            "NVARCHAR",
            "NVARCHAR2",
            "CHAR VARYING",
            "NCHAR VARYING",
        },
        {
            "NUMBER",
            "DECIMAL",
            "DEC",
            "NUMERIC",
            "FIXED",
            "INT",
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "TINYINT",
            "BYTEINT",
        },
        {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"},
        {"DATETIME", "TIMESTAMP_NTZ"},
    )

    def correct_for_autogen_constraints(
        self,
        conn_uniques: set[UniqueConstraint],
        conn_indexes: set[Index],
        metadata_unique_constraints: set[UniqueConstraint],
        metadata_indexes: set[Index],
    ) -> None:
        """Drop hybrid tables' implicit FK backing indexes from the comparison.

        Hybrid tables materialize each FOREIGN KEY as an implicit backing
        index named after the constraint.
        """
        for idx in list(conn_indexes):
            if idx.table is not None and idx.name in {
                fk.name for fk in idx.table.foreign_key_constraints
            }:
                conn_indexes.discard(idx)

    def _exec(self, construct: Any, *args: Any, **kw: Any) -> Any:
        """Only one index can be building for a table at a time,
        and `CREATE INDEX` does not on its own await index builds.

        Hooked here rather than in `create_index` because `create_table`
        emits its tables' indexes directly through `_exec`.
        """
        if isinstance(construct, CreateIndex) and construct.element.table is not None:
            self._await_index_builds(construct.element.table)
        return super()._exec(construct, *args, **kw)

    def _await_index_builds(self, table: Table) -> None:
        if self.connection is None:  # offline mode
            return
        qualified = self.connection.dialect.identifier_preparer.format_table(table)
        deadline = (
            time.monotonic() + self.index_build_timeout
            if self.index_build_timeout is not None
            else None
        )
        while True:
            rows = (
                self.connection.exec_driver_sql(f"SHOW INDEXES IN TABLE {qualified}")
                .mappings()
                .all()
            )
            building = [r["name"] for r in rows if r["status"] != "ACTIVE"]
            if not building:
                return
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError(
                    f"index build(s) still in progress on {qualified} after"
                    f" {self.index_build_timeout}s: {building}"
                )
            time.sleep(self.index_build_poke_interval)
