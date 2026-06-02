#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

from enum import Enum
from typing import Any

from snowflake.sqlalchemy.custom_commands import NoneType

from .keyword_option import KeywordOption, KeywordOptionType
from .keywords import SnowflakeKeyword
from .table_option import Priority, TableOption, TableOptionKey


class TimeUnit(Enum):
    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"


class TargetLagOption(TableOption):
    """Class to represent the target lag clause in Dynamic Tables.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table

    Example using the time and unit parameters:

        target_lag = TargetLagOption(10, TimeUnit.SECONDS)

        is equivalent to:

        TARGET_LAG = '10 SECONDS'

    Example using keyword parameter:

        target_lag = KeywordOption(SnowflakeKeyword.DOWNSTREAM)

        is equivalent to:

        TARGET_LAG = DOWNSTREAM

    """

    def __init__(
        self,
        time: int = 0,
        unit: TimeUnit = TimeUnit.MINUTES,
    ) -> None:
        super().__init__()
        self.time = time
        self.unit = unit
        self._name: TableOptionKey = TableOptionKey.TARGET_LAG

    @staticmethod
    def create(  # type: ignore[override]
        value: TargetLagOption | tuple[int, TimeUnit] | KeywordOptionType,
    ) -> TableOption | None:
        if isinstance(value, NoneType):
            return value

        if isinstance(value, tuple):
            time, unit = value
            value = TargetLagOption(time, unit)

        if isinstance(value, TargetLagOption):
            return value

        if isinstance(value, (KeywordOption, SnowflakeKeyword)):
            return KeywordOption.create(TableOptionKey.TARGET_LAG, value)

        return TableOption._get_invalid_table_option(
            TableOptionKey.TARGET_LAG,
            str(type(value).__name__),
            [
                TargetLagOption.__name__,
                f"tuple[int, {TimeUnit.__name__}]",
                SnowflakeKeyword.__name__,
            ],
        )

    def __get_expression(self) -> str:
        return f"'{str(self.time)} {str(self.unit.value)}'"

    @property
    def priority(self) -> Priority:
        return Priority.HIGH

    def _render(self, compiler: Any) -> str:
        return self.template() % (self.__get_expression())

    def __repr__(self) -> str:
        return "TargetLagOption(%s)" % self.__get_expression()


TargetLagOptionType = TargetLagOption | tuple[int, TimeUnit]
