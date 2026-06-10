#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class LiteralOption(TableOption):
    """Class to represent a literal option in Snowflake Table.

    Example:
        warehouse = LiteralOption('my_warehouse')

        is equivalent to:

        WAREHOUSE = 'my_warehouse'
    """

    def __init__(self, value: int | str) -> None:
        super().__init__()
        self.value: Any = value

    @property
    def priority(self) -> Priority:
        return Priority.HIGH

    @staticmethod
    def create(  # type: ignore[override]
        name: TableOptionKey,
        value: str | int | LiteralOption | None,
    ) -> TableOption | None:
        if isinstance(value, NoneType):
            return None
        if isinstance(value, (str, int)):
            value = LiteralOption(value)

        if isinstance(value, LiteralOption):
            value._set_option_name(name)
            return value

        return TableOption._get_invalid_table_option(
            name,
            str(type(value).__name__),
            [LiteralOption.__name__, str.__name__, int.__name__],
        )

    def template(self) -> str:
        name = self.option_name
        assert name is not None, f"option_name not set on {self.__class__.__name__}"
        if isinstance(self.value, int):
            return f"{name.upper()} = %d"
        else:
            return f"{name.upper()} = '%s'"

    def _render(self, compiler: Any) -> str:
        if isinstance(self.value, int):
            return self.template() % self.value
        escaped = self._escape_string_literal_value(str(self.value))
        return self.template() % escaped

    def __repr__(self) -> str:
        option_name = (
            f", table_option_key={self.option_name}"
            if not isinstance(self.option_name, NoneType)
            else ""
        )
        return f"LiteralOption(value='{self.value}'{option_name})"


LiteralOptionType = LiteralOption | str | int
