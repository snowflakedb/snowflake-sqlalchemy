#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from . import base
from . import snowdialect
from .custom_commands import (
    MergeInto, CSVFormatter, JSONFormatter, PARQUETFormatter, CopyIntoStorage, AWSBucket, AzureContainer
)
from .util import _url as URL
from .version import VERSION
from snowflake.connector.compat import TO_UNICODE
from .custom_types import VARIANT, ARRAY, OBJECT, TIMESTAMP_LTZ, TIMESTAMP_TZ, TIMESTAMP_NTZ


SNOWFLAKE_CONNECTOR_VERSION = '.'.join(TO_UNICODE(v) for v in VERSION[0:3])


base.dialect = dialect = snowdialect.dialect

__version__ = SNOWFLAKE_CONNECTOR_VERSION

__all__ = (
    'VARIANT',
    'ARRAY',
    'OBJECT',
    'TIMESTAMP_LTZ',
    'TIMESTAMP_TZ',
    'TIMESTAMP_NTZ',
    'MergeInto',
    'CSVFormatter',
    'JSONFormatter',
    'PARQUETFormatter',
    'CopyIntoStorage',
    'AWSBucket',
    'AzureContainer'
)
