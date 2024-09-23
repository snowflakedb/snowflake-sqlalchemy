#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
# from enum import Enum
from enum import Enum
from typing import Optional

from .table_option import TableOption
from .table_option_base import Priority


class TimeUnit(Enum):
    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hour"
    DAYS = "days"


class TargetLag(TableOption):
    """Class to represent the target lag clause.
    This configuration option is used to specify the target lag time for the dynamic table.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table


    Target lag example usage:
    DynamicTable("sometable", metadata,
                    Column("name", String(50)),
                    Column("address", String(100)),
                    TargetLag(20, TimeUnit.MINUTES),
                    )
    """

    __option_name__ = "target_lag"
    __priority__ = Priority.HIGH

    def __init__(
        self,
        time: Optional[int] = 0,
        unit: Optional[TimeUnit] = TimeUnit.MINUTES,
        down_stream: Optional[bool] = False,
    ) -> None:
        self.time = time
        self.unit = unit
        self.down_stream = down_stream

    @staticmethod
    def template() -> str:
        return "TARGET_LAG = %s"

    def get_expression(self):
        return (
            ("'" + str(self.time) + " " + str(self.unit.value) + "'")
            if not self.down_stream
            else "DOWNSTREAM"
        )

    def render_option(self, compiler) -> str:
        return TargetLag.template() % (self.get_expression())

    def __repr__(self) -> str:
        return "TargetLag(%s)" % self.get_expression()
