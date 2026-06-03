#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING

from snowflake.sqlalchemy.custom_commands import NoneType

from .keywords import SnowflakeKeyword
from .table_option import Priority, TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class KeywordOption(TableOption):
    """Class to represent a keyword option in Snowflake Tables.

    Example:
        target_lag = KeywordOption(SnowflakeKeyword.DOWNSTREAM)

        is equivalent to:

        TARGET_LAG = DOWNSTREAM
    """

    def __init__(self, value: SnowflakeKeyword) -> None:
        super().__init__()
        self.value: str = value.value

    @property
    def priority(self) -> Priority:
        return Priority.HIGH

    def template(self) -> str:
        name = self.option_name
        assert name is not None, f"option_name not set on {self.__class__.__name__}"
        return f"{name.upper()} = %s"

    def _render(self, compiler: Any) -> str:
        # This function renders only keywords, so no additional processing is needed.
        return self.template() % self.value.upper()

    @staticmethod
    def create(  # type: ignore[override]
        name: TableOptionKey,
        value: SnowflakeKeyword | KeywordOption | None,
    ) -> TableOption | None:
        if isinstance(value, NoneType):
            return value
        if isinstance(value, SnowflakeKeyword):
            value = KeywordOption(value)

        if isinstance(value, KeywordOption):
            value._set_option_name(name)
            return value

        return TableOption._get_invalid_table_option(
            name,
            str(type(value).__name__),
            [KeywordOption.__name__, SnowflakeKeyword.__name__],
        )

    def __repr__(self) -> str:
        option_name = (
            f", table_option_key={self.option_name}"
            if isinstance(self.option_name, NoneType)
            else ""
        )
        return f"KeywordOption(value='{self.value}'{option_name})"


KeywordOptionType = KeywordOption | SnowflakeKeyword
