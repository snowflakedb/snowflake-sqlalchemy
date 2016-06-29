#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2016 Snowflake Computing Inc. All right reserved.
#

from .version import VERSION
from ..connector.compat import (TO_UNICODE)

SNOWFLAKE_CONNECTOR_VERSION = '.'.join(TO_UNICODE(v) for v in VERSION[0:3])

__version__ = SNOWFLAKE_CONNECTOR_VERSION

from .util import _url as URL
