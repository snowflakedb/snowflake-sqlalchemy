#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING

from .table_option import TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class InvalidTableOption(TableOption):
    """Class to store errors and raise them after table initialization in order to avoid recursion error."""

    def __init__(self, name: TableOptionKey, value: Exception) -> None:
        super().__init__()
        self.exception: Exception = value
        self._name = name

    @staticmethod
    def create(  # type: ignore[override]
        name: TableOptionKey, value: Exception
    ) -> TableOption:
        return InvalidTableOption(name, value)

    def _render(self, compiler: SnowflakeDDLCompiler) -> str:
        raise self.exception

    def __repr__(self) -> str:
        return f"ErrorOption(value='{self.exception}')"
