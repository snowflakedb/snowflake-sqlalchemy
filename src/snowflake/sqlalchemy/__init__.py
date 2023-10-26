#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import sys

if sys.version_info < (3, 8):
    import importlib_metadata
else:
    import importlib.metadata as importlib_metadata

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
    GEOGRAPHY,
    GEOMETRY,
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

base.dialect = dialect = snowdialect.dialect

__version__ = importlib_metadata.version("snowflake-sqlalchemy")

__all__ = (
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "FLOAT",
    "INT",
    "INTEGER",
    "REAL",
    "SMALLINT",
    "TIME",
    "TIMESTAMP",
    "URL",
    "VARCHAR",
    "ARRAY",
    "BYTEINT",
    "CHARACTER",
    "DEC",
    "DOUBLE",
    "FIXED",
    "GEOGRAPHY",
    "GEOMETRY",
    "OBJECT",
    "NUMBER",
    "STRING",
    "TEXT",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIMESTAMP_NTZ",
    "TINYINT",
    "VARBINARY",
    "VARIANT",
    "MergeInto",
    "CSVFormatter",
    "JSONFormatter",
    "PARQUETFormatter",
    "CopyFormatter",
    "CopyIntoStorage",
    "AWSBucket",
    "AzureContainer",
    "ExternalStage",
    "CreateStage",
    "CreateFileFormat",
)

def snowflake_sqlalchemy_20_monkey_patches():
    """Ensure Snowflake integrates with sqlalchemy 2.0."""
    import sqlalchemy.util.compat

    # make strings always return unicode strings
    sqlalchemy.util.compat.string_types = (str,)
    sqlalchemy.types.String.RETURNS_UNICODE = True

    import snowflake.sqlalchemy.snowdialect

    snowflake.sqlalchemy.snowdialect.SnowflakeDialect.returns_unicode_strings = True

    # make has_table() support the `info_cache` kwarg
    import snowflake.sqlalchemy.snowdialect

    def has_table(self, connection, table_name, schema=None, info_cache=None):
        """
        Checks if the table exists
        """
        return self._has_object(connection, "TABLE", table_name, schema)

    snowflake.sqlalchemy.snowdialect.SnowflakeDialect.has_table = has_table


# usage: call this function before creating an engine:
snowflake_sqlalchemy_20_monkey_patches()
