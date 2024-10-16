#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from enum import Enum


class CustomTablePrefix(Enum):
    DEFAULT = 0
    EXTERNAL = 1
    EVENT = 2
    HYBRID = 3
    ICEBERG = 4
    DYNAMIC = 5
