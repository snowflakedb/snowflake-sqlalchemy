#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Any, Optional, Union

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey


class LiteralOption(TableOption):
    """Class to represent a literal option in Snowflake Table.

    Example:
        warehouse = LiteralOption('my_warehouse')

        is equivalent to:

        WAREHOUSE = 'my_warehouse'
    """

    def __init__(self, value: Union[int, str]) -> None:
        super().__init__()
        self.value: Any = value

    @property
    def priority(self):
        return Priority.HIGH

    @staticmethod
    def create(
        name: TableOptionKey, value: Optional[Union[str, int, "LiteralOption"]]
    ) -> Optional[TableOption]:
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
        if isinstance(self.value, int):
            return f"{self.option_name.upper()} = %d"
        else:
            return f"{self.option_name.upper()} = '%s'"

    def _render(self, compiler) -> str:
        return self.template() % self.value

    def __repr__(self) -> str:
        option_name = (
            f", table_option_key={self.option_name}"
            if not isinstance(self.option_name, NoneType)
            else ""
        )
        return f"LiteralOption(value='{self.value}'{option_name})"


LiteralOptionType = Union[LiteralOption, str, int]
