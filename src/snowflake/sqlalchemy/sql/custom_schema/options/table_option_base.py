#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from abc import ABC, abstractmethod
from enum import Enum


class Priority(Enum):
    LOWEST = 0
    VERY_LOW = 1
    LOW = 2
    MEDIUM = 4
    HIGH = 6
    VERY_HIGH = 7
    HIGHEST = 8


class TableOptionBase(ABC):
    __option_name__ = "default"
    __visit_name__ = __option_name__
    __priority__ = Priority.MEDIUM

    @staticmethod
    @abstractmethod
    def template() -> str:
        pass

    @abstractmethod
    def get_expression(self):
        pass

    @abstractmethod
    def render_option(self, compiler) -> str:
        pass
