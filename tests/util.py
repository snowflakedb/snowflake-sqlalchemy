#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import random
import re
import string
from collections.abc import Sequence

import pytest
import sqlalchemy
from sqlalchemy.sql.sqltypes import UUID
from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    REAL,
    SMALLINT,
    TIME,
    TIMESTAMP,
    VARCHAR,
    Integer,
)

from snowflake.sqlalchemy.custom_types import (
    ARRAY,
    DECFLOAT,
    GEOGRAPHY,
    GEOMETRY,
    MAP,
    OBJECT,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
    VECTOR,
)

ischema_names_baseline = {
    "BIGINT": BIGINT,
    "BINARY": BINARY,
    # 'BIT': BIT,
    "BOOLEAN": BOOLEAN,
    "CHAR": CHAR,
    "CHARACTER": CHAR,
    "DATE": DATE,
    "DATETIME": DATETIME,
    "DEC": DECIMAL,
    "DECIMAL": DECIMAL,
    "DECFLOAT": DECFLOAT,
    "DOUBLE": FLOAT,
    "FIXED": DECIMAL,
    "FLOAT": FLOAT,
    "INT": Integer,
    "INTEGER": Integer,
    "NUMBER": DECIMAL,
    # 'OBJECT': ?
    "REAL": REAL,
    "BYTEINT": SMALLINT,
    "SMALLINT": SMALLINT,
    "STRING": VARCHAR,
    "TEXT": VARCHAR,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
    "TIMESTAMP_TZ": TIMESTAMP_TZ,
    "TIMESTAMP_LTZ": TIMESTAMP_LTZ,
    "TIMESTAMP_NTZ": TIMESTAMP_NTZ,
    "TINYINT": SMALLINT,
    "VARBINARY": BINARY,
    "VARCHAR": VARCHAR,
    "VARIANT": VARIANT,
    "VECTOR": VECTOR,
    "OBJECT": OBJECT,
    "ARRAY": ARRAY,
    "GEOGRAPHY": GEOGRAPHY,
    "GEOMETRY": GEOMETRY,
    "MAP": MAP,
    "UUID": UUID,
}


def random_string(
    length: int,
    prefix: str = "",
    suffix: str = "",
    choices: Sequence[str] = string.ascii_lowercase,
) -> str:
    """Our convenience function to generate random string for object names.

    Args:
        length: How many random characters to choose from choices.
        prefix: Prefix to add to random string generated.
        suffix: Suffix to add to random string generated.
        choices: A generator of things to choose from.
    """
    random_part = "".join([random.choice(choices) for _ in range(length)])
    return "".join([prefix, random_part, suffix])


def normalize_ddl(ddl: str) -> str:
    """Normalize DDL string by removing extra whitespace and newlines."""
    return re.sub(r"\s+", " ", ddl).strip()


def compile_type(type_instance):
    """Compile a type to its DDL string using the Snowflake type compiler."""
    from snowflake.sqlalchemy import snowdialect

    dialect = snowdialect.dialect()
    return dialect.type_compiler.process(type_instance)


SQLALCHEMY_VERSION = tuple(map(int, sqlalchemy.__version__.split(".")[:2]))
SA21 = SQLALCHEMY_VERSION >= (2, 1)

# Mark a test that pins behaviour of one SQLAlchemy line (2.0 or 2.1); the
# other line has a same-named ``*_sa20`` / ``*_sa21`` counterpart.
sa21_only = pytest.mark.skipif(not SA21, reason="SQLAlchemy 2.1 behaviour/API")
sa20_only = pytest.mark.skipif(SA21, reason="SQLAlchemy 2.0 behaviour")


def compile_sql(stmt, dialect, literal_binds: bool = False) -> str:
    """Compile ``stmt`` for ``dialect`` and collapse whitespace."""
    compiled = stmt.compile(
        dialect=dialect, compile_kwargs={"literal_binds": literal_binds}
    )
    return " ".join(str(compiled).split())
