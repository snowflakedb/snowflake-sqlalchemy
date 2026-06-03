#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class IdentifierOption(TableOption):
    """Class to represent an identifier option in Snowflake Tables.

    Example:
        warehouse = IdentifierOption('my_warehouse')

        is equivalent to:

        WAREHOUSE = my_warehouse
    """

    def __init__(self, value: str) -> None:
        super().__init__()
        self.value: str = value

    @property
    def priority(self) -> Priority:
        return Priority.HIGH

    @staticmethod
    def create(  # type: ignore[override]
        name: TableOptionKey,
        value: str | IdentifierOption | None,
    ) -> TableOption | None:
        if isinstance(value, NoneType):
            return None

        if isinstance(value, str):
            value = IdentifierOption(value)

        if isinstance(value, IdentifierOption):
            value._set_option_name(name)
            return value

        return TableOption._get_invalid_table_option(
            name, str(type(value).__name__), [IdentifierOption.__name__, str.__name__]
        )

    def template(self) -> str:
        name = self.option_name
        assert name is not None, f"option_name not set on {self.__class__.__name__}"
        return f"{name.upper()} = %s"

    def _render(self, compiler: SnowflakeDDLCompiler) -> str:
        return self.template() % self.value

    def __repr__(self) -> str:
        option_name = (
            f", table_option_key={self.option_name}"
            if not isinstance(self.option_name, NoneType)
            else ""
        )
        return f"IdentifierOption(value='{self.value}'{option_name})"


IdentifierOptionType = IdentifierOption | str
