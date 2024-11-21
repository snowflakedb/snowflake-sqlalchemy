#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional, Union

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey


class IdentifierOption(TableOption):
    """Class to represent an identifier option in Snowflake Tables.

    Example:
        warehouse = IdentifierOption('my_warehouse')

        is equivalent to:

        WAREHOUSE = my_warehouse
    """

    def __init__(self, value: Union[str]) -> None:
        super().__init__()
        self.value: str = value

    @property
    def priority(self):
        return Priority.HIGH

    @staticmethod
    def create(
        name: TableOptionKey, value: Optional[Union[str, "IdentifierOption"]]
    ) -> Optional[TableOption]:
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
        return f"{self.option_name.upper()} = %s"

    def _render(self, compiler) -> str:
        return self.template() % self.value

    def __repr__(self) -> str:
        option_name = (
            f", table_option_key={self.option_name}"
            if not isinstance(self.option_name, NoneType)
            else ""
        )
        return f"IdentifierOption(value='{self.value}'{option_name})"


IdentifierOptionType = Union[IdentifierOption, str, int]
