#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum


class Priority(Enum):
    LOWEST = 0
    VERY_LOW = 1
    LOW = 2
    MEDIUM = 4
    HIGH = 6
    VERY_HIGH = 7
    HIGHEST = 8


class TableOptionBase:
    __option_name__ = "default"
    __visit_name__ = __option_name__
    __priority__ = Priority.MEDIUM

    @staticmethod
    def template() -> str:
        raise NotImplementedError

    def get_expression(self):
        raise NotImplementedError

    def render_option(self, compiler) -> str:
        raise NotImplementedError
