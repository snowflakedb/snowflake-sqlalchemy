#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import sqlalchemy.types as sqltypes

TEXT = sqltypes.VARCHAR
CHARACTER = sqltypes.CHAR
DEC = sqltypes.DECIMAL
DOUBLE = sqltypes.FLOAT
FIXED = sqltypes.DECIMAL
NUMBER = sqltypes.DECIMAL
BYTEINT = sqltypes.SMALLINT
STRING = sqltypes.VARCHAR
TINYINT = sqltypes.SMALLINT
VARBINARY = sqltypes.BINARY


class SnowflakeType(sqltypes.TypeEngine):

    def _default_dialect(self):
        # Get around circular import
        return __import__('snowflake.sqlalchemy').sqlalchemy.dialect()


class VARIANT(SnowflakeType):
    __visit_name__ = 'VARIANT'


class OBJECT(SnowflakeType):
    __visit_name__ = 'OBJECT'


class ARRAY(SnowflakeType):
    __visit_name__ = 'ARRAY'


class TIMESTAMP_TZ(SnowflakeType):
    __visit_name__ = 'TIMESTAMP_TZ'


class TIMESTAMP_LTZ(SnowflakeType):
    __visit_name__ = 'TIMESTAMP_LTZ'


class TIMESTAMP_NTZ(SnowflakeType):
    __visit_name__ = 'TIMESTAMP_NTZ'
