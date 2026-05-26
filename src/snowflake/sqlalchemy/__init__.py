#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from importlib.metadata import version as _get_version

from sqlalchemy.sql.sqltypes import UUID  # noqa
from sqlalchemy.types import (  # noqa
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

from . import base, snowdialect  # noqa
from .custom_commands import (  # noqa
    AWSBucket,
    AzureContainer,
    CloudStorageLocation,
    CopyFormatter,
    CopyIntoStorage,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    GCSBucket,
    JSONFormatter,
    MergeInto,
    PARQUETFormatter,
)
from .custom_types import (  # noqa
    ARRAY,
    BYTEINT,
    CHARACTER,
    DEC,
    DECFLOAT,
    DOUBLE,
    FIXED,
    GEOGRAPHY,
    GEOMETRY,
    MAP,
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
    VECTOR,
)
from .orm import SnowflakeBase, SnowflakeSession, snowflake_declarative_base  # noqa
from .sql.custom_schema import (  # noqa
    DynamicTable,
    HybridTable,
    IcebergTable,
    SnowflakeTable,
)
from .sql.custom_schema.options import (  # noqa
    AsQueryOption,
    ClusterByOption,
    IdentifierOption,
    KeywordOption,
    LiteralOption,
    SnowflakeKeyword,
    TableOptionKey,
    TargetLagOption,
    TimeUnit,
)
from .util import _url as URL  # noqa
from .util import create_snowflake_engine  # noqa

base.dialect = dialect = snowdialect.dialect

__version__ = _get_version("snowflake-sqlalchemy")

_custom_types = (
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "CHAR",
    "DATE",
    "DATETIME",
    "DECIMAL",
    "DECFLOAT",
    "FLOAT",
    "INT",
    "INTEGER",
    "REAL",
    "SMALLINT",
    "TIME",
    "TIMESTAMP",
    "URL",
    "UUID",
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
    "VECTOR",
    "MAP",
)

_custom_commands = (
    "MergeInto",
    "CSVFormatter",
    "JSONFormatter",
    "PARQUETFormatter",
    "CopyFormatter",
    "CopyIntoStorage",
    "CloudStorageLocation",
    "AWSBucket",
    "AzureContainer",
    "GCSBucket",
    "ExternalStage",
    "CreateStage",
    "CreateFileFormat",
)

_custom_tables = ("HybridTable", "DynamicTable", "IcebergTable", "SnowflakeTable")

_custom_table_options = (
    "AsQueryOption",
    "TargetLagOption",
    "LiteralOption",
    "IdentifierOption",
    "KeywordOption",
    "ClusterByOption",
)

_enums = (
    "TimeUnit",
    "TableOptionKey",
    "SnowflakeKeyword",
)

_orm = (
    "SnowflakeBase",
    "SnowflakeSession",
    "snowflake_declarative_base",
)

_helpers = ("create_snowflake_engine",)

__all__ = (
    *_custom_types,
    *_custom_commands,
    *_custom_tables,
    *_custom_table_options,
    *_enums,
    *_orm,
    *_helpers,
)
