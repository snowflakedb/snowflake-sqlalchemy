#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
#

from snowflake.sqlalchemy.base import (
    VARIANT, ARRAY, OBJECT, TIMESTAMP_LTZ, TIMESTAMP_TZ, TIMESTAMP_NTZ)
from snowflake.sqlalchemy.util import _url as URL
from snowflake.sqlalchemy.version import VERSION
from snowflake.connector.compat import (TO_UNICODE)

SNOWFLAKE_CONNECTOR_VERSION = '.'.join(TO_UNICODE(v) for v in VERSION[0:3])

__version__ = SNOWFLAKE_CONNECTOR_VERSION
