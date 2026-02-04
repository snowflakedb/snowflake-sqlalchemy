#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from enum import Enum


class SnowflakeKeyword(Enum):
    # TARGET_LAG
    DOWNSTREAM = "DOWNSTREAM"

    # REFRESH_MODE
    AUTO = "AUTO"
    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
