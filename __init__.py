#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from . import base
from . import snowdialect
from .custom_commands import (
    MergeInto,
    CSVFormatter,
    JSONFormatter,
    PARQUETFormatter,
    CopyIntoStorage,
    AWSBucket,
    AzureContainer,
    ExternalStage
)
from .util import _url as URL
from .version import VERSION
from snowflake.connector.compat import TO_UNICODE
from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INT,
    INTEGER,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    VARCHAR,
)
from .custom_types import (
    ARRAY,
    BYTEINT,
    CHARACTER,
    DEC,
    DOUBLE,
    FIXED,
    OBJECT,
    NUMBER,
    STRING,
    TEXT,
    TIMESTAMP_LTZ,
    TIMESTAMP_TZ,
    TIMESTAMP_NTZ,
    TINYINT,
    VARBINARY,
    VARIANT,
)

SNOWFLAKE_CONNECTOR_VERSION = '.'.join(TO_UNICODE(v) for v in VERSION[0:3])

base.dialect = dialect = snowdialect.dialect

__version__ = SNOWFLAKE_CONNECTOR_VERSION

__all__ = (
    'BIGINT',
    'BINARY',
    'BOOLEAN',
    'CHAR',
    'DATE',
    'DATETIME',
    'DECIMAL',
    'FLOAT',
    'INT',
    'INTEGER',
    'REAL',
    'SMALLINT',
    'TIME',
    'TIMESTAMP',
    'URL',
    'VARCHAR',

    'ARRAY',
    'BYTEINT',
    'CHARACTER',
    'DEC',
    'DOUBLE',
    'FIXED',
    'OBJECT',
    'NUMBER',
    'STRING',
    'TEXT',
    'TIMESTAMP_LTZ',
    'TIMESTAMP_TZ',
    'TIMESTAMP_NTZ',
    'TINYINT',
    'VARBINARY',
    'VARIANT',

    'MergeInto',
    'CSVFormatter',
    'JSONFormatter',
    'PARQUETFormatter',
    'CopyIntoStorage',
    'AWSBucket',
    'AzureContainer',
    'ExternalStage',
)
