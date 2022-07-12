#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import datetime
import decimal
import re

import sqlalchemy.types as sqltypes
import sqlalchemy.util as util

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


def _process_float(value):
    if value == float("inf"):
        return "inf"
    elif value == float("-inf"):
        return "-inf"
    elif value is not None:
        return float(value)
    return value


class SnowflakeType(sqltypes.TypeEngine):
    def _default_dialect(self):
        # Get around circular import
        return __import__("snowflake.sqlalchemy").sqlalchemy.dialect()


class VARIANT(SnowflakeType):
    __visit_name__ = "VARIANT"


class OBJECT(SnowflakeType):
    __visit_name__ = "OBJECT"


class ARRAY(SnowflakeType):
    __visit_name__ = "ARRAY"


class TIMESTAMP_TZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_TZ"


class TIMESTAMP_LTZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_LTZ"


class TIMESTAMP_NTZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_NTZ"


class GEOGRAPHY(SnowflakeType):
    __visit_name__ = "GEOGRAPHY"


class _CUSTOM_Date(SnowflakeType, sqltypes.Date):
    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                return f"'{value.isoformat()}'"

        return process

    _reg = re.compile(r"(\d+)-(\d+)-(\d+)")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(f"could not parse {value!r} as a date value")
                return datetime.date(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class _CUSTOM_DateTime(SnowflakeType, sqltypes.DateTime):
    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                datetime_str = value.isoformat(" ", timespec="microseconds")
                return f"'{datetime_str}'"

        return process

    _reg = re.compile(r"(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d{0,6}))?")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(f"could not parse {value!r} as a datetime value")
                return datetime.datetime(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class _CUSTOM_Time(SnowflakeType, sqltypes.Time):
    def literal_processor(self, dialect):
        def process(value):
            if value is not None:
                time_str = value.isoformat(timespec="microseconds")
                return f"'{time_str}'"

        return process

    _reg = re.compile(r"(\d+):(\d+):(\d+)(?:\.(\d{0,6}))?")

    def result_processor(self, dialect, coltype):
        def process(value):
            if isinstance(value, str):
                m = self._reg.match(value)
                if not m:
                    raise ValueError(f"could not parse {value!r} as a time value")
                return datetime.time(*[int(x or 0) for x in m.groups()])
            else:
                return value

        return process


class _CUSTOM_Float(SnowflakeType, sqltypes.Float):
    def bind_processor(self, dialect):
        return _process_float


class _CUSTOM_DECIMAL(SnowflakeType, sqltypes.DECIMAL):
    @util.memoized_property
    def _type_affinity(self):
        return sqltypes.INTEGER if self.scale == 0 else sqltypes.DECIMAL


class _CUSTOM_Numeric(SnowflakeType, sqltypes.Numeric):
    def result_processor(self, dialect, coltype):
        if self.asdecimal:

            def process(value):
                if value:
                    return decimal.Decimal(value)
                else:
                    return None

            return process
        else:
            return _process_float
