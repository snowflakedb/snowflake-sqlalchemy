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
TINYINT  = sqltypes.SMALLINT
VARBINARY = sqltypes.BINARY

class VARIANT(sqltypes.TypeEngine):
    __visit_name__ = 'VARIANT'


class OBJECT(sqltypes.TypeEngine):
    __visit_name__ = 'OBJECT'


class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = 'ARRAY'


class TIMESTAMP_TZ(sqltypes.TIMESTAMP):
    __visit_name__ = 'TIMESTAMP_TZ'


class TIMESTAMP_LTZ(sqltypes.TIMESTAMP):
    __visit_name__ = 'TIMESTAMP_LTZ'


class TIMESTAMP_NTZ(sqltypes.TIMESTAMP):
    __visit_name__ = 'TIMESTAMP_NTZ'
