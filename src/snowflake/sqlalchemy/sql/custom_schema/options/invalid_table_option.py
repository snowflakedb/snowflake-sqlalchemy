#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional

from .table_option import TableOption, TableOptionKey


class InvalidTableOption(TableOption):
    """Class to store errors and raise them after table initialization in order to avoid recursion error."""

    def __init__(self, name: TableOptionKey, value: Exception) -> None:
        super().__init__()
        self.exception: Exception = value
        self._name = name

    @staticmethod
    def create(name: TableOptionKey, value: Exception) -> Optional[TableOption]:
        return InvalidTableOption(name, value)

    def _render(self, compiler) -> str:
        raise self.exception

    def __repr__(self) -> str:
        return f"ErrorOption(value='{self.exception}')"
