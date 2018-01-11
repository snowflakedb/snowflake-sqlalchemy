#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
#

from .base import (VARIANT, ARRAY, OBJECT)
from .util import _url as URL
from .version import VERSION
from ..connector.compat import (TO_UNICODE)

SNOWFLAKE_CONNECTOR_VERSION = '.'.join(TO_UNICODE(v) for v in VERSION[0:3])

__version__ = SNOWFLAKE_CONNECTOR_VERSION
