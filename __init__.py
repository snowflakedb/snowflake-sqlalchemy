#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

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

from . import base, snowdialect
from .custom_commands import (
    AWSBucket,
    AzureContainer,
    CopyFormatter,
    CopyIntoStorage,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    JSONFormatter,
    MergeInto,
    PARQUETFormatter,
)
from .custom_types import (
    ARRAY,
    BYTEINT,
    CHARACTER,
    DEC,
    DOUBLE,
    FIXED,
    NUMBER,
    OBJECT,
    STRING,
    TEXT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    TINYINT,
    VARBINARY,
    VARIANT,
)
from .util import _url as URL
from .version import VERSION

SNOWFLAKE_CONNECTOR_VERSION = '.'.join(str(v) for v in VERSION[0:3])

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
    'CopyFormatter',
    'CopyIntoStorage',
    'AWSBucket',
    'AzureContainer',
    'ExternalStage',
    'CreateStage',
    'CreateFileFormat',
)
