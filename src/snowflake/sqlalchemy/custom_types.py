#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

import decimal
import json
import keyword
import warnings
from collections.abc import Callable
from datetime import date, datetime, time
from typing import TYPE_CHECKING, Any, ClassVar

import sqlalchemy.types as sqltypes
import sqlalchemy.util as util
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ColumnElement
from sqlalchemy.sql.visitors import InternalTraversal
from sqlalchemy.types import TypeEngine

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import Dialect

    from .snowdialect import SnowflakeDialect

DECFLOAT_PRECISION = 38

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


def _process_float(value: float | None) -> float | str | None:
    if value == float("inf"):
        return "inf"
    elif value == float("-inf"):
        return "-inf"
    elif value is not None:
        return float(value)
    return value


class SnowflakeType(sqltypes.TypeEngine):
    def _default_dialect(self) -> SnowflakeDialect:
        # Get around circular import — SnowflakeDialect is only imported under TYPE_CHECKING
        return __import__("snowflake.sqlalchemy").sqlalchemy.dialect()


class _ParseJSONBind(ColumnElement):
    """Wrap a bound semi-structured value so it renders as ``PARSE_JSON(:p)``.

    ``TypeEngine.bind_expression`` has no access to the dialect, so the wrapping
    element defers the decision to render ``PARSE_JSON`` to compile time, where
    ``compiler.dialect`` is available. On Snowflake with
    ``enable_structured_type_json`` set it renders ``PARSE_JSON(<inner>)``;
    otherwise (flag off, or a non-Snowflake dialect) it renders the bare value,
    so the presence of this wrapper is a no-op by default.
    """

    inherit_cache = True
    # Without an explicit traversal, a custom ColumnElement falls back to
    # NO_CACHE, which disables statement caching for every INSERT/UPDATE/WHERE
    # that binds a value to a semi-structured column — even when the flag is off
    # (bind_expression wraps unconditionally). Traversing ``wrapped`` keeps such
    # statements cacheable; the dialect flag is fixed per engine, so it need not
    # be part of the key.
    _cache_key_traversal = [("wrapped", InternalTraversal.dp_clauseelement)]

    def __init__(self, wrapped: ColumnElement) -> None:
        self.wrapped = wrapped
        self.type = sqltypes.NULLTYPE


@compiles(_ParseJSONBind)
def _render_parse_json_bind_default(element: _ParseJSONBind, compiler, **kw) -> str:
    return compiler.process(element.wrapped, **kw)


@compiles(_ParseJSONBind, "snowflake")
def _render_parse_json_bind_snowflake(element: _ParseJSONBind, compiler, **kw) -> str:
    inner = compiler.process(element.wrapped, **kw)
    if getattr(compiler.dialect, "_enable_structured_type_json", False):
        return f"PARSE_JSON({inner})"
    return inner


class _SemiStructuredJSONMixin:
    """Opt-in JSON deserialization for semi-structured columns.

    When ``enable_structured_type_json`` is set on the dialect, reading a
    VARIANT / OBJECT / ARRAY / MAP column deserializes the JSON text Snowflake
    returns into native Python (``dict`` / ``list`` / ...). The dialect's
    ``json_deserializer`` is used when provided, otherwise ``json.loads``.

    The flag defaults to off, so without it these types behave exactly as
    before (raw passthrough) and existing code is unaffected — no BCR.
    """

    def result_processor(
        self, dialect: Dialect, coltype: object
    ) -> Callable[[Any], Any] | None:
        if not getattr(dialect, "_enable_structured_type_json", False):
            return None
        # Scope deserialization to the semi-structured (untyped) form, matching
        # the write path and the documented behavior. Typed/structured columns
        # (OBJECT(a=...), ARRAY(<type>), MAP) keep their native connector handling.
        if not self._is_untyped_semi_structured():
            return None

        deserializer = getattr(dialect, "_json_deserializer", None) or json.loads

        def process(value: Any) -> Any:
            # Only decode the textual form Snowflake returns for semi-structured
            # columns; leave None and any already-parsed value untouched.
            if isinstance(value, (str, bytes, bytearray)):
                return deserializer(value)
            return value

        return process

    def _is_untyped_semi_structured(self) -> bool:
        # Semi-structured (untyped) when no element/field/key typing is declared:
        # VARIANT, OBJECT() and ARRAY(). Typed forms — OBJECT(a=...), ARRAY(<type>)
        # and MAP(k, v) — are structured and keep their native connector handling.
        if getattr(self, "items_types", None):
            return False
        if getattr(self, "value_type", None) is not None:
            return False
        if getattr(self, "key_type", None) is not None:
            return False
        return True

    def _json_write_enabled(self, dialect: Dialect) -> bool:
        # The PARSE_JSON write path only applies to the semi-structured (untyped)
        # form; typed/structured columns keep their native connector handling.
        return (
            getattr(dialect, "_enable_structured_type_json", False)
            and self._is_untyped_semi_structured()
        )

    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Any] | None:
        if not self._json_write_enabled(dialect):
            return None

        serializer = getattr(dialect, "_json_serializer", None) or json.dumps

        def process(value: Any) -> Any:
            # Serialize native Python objects to JSON text for PARSE_JSON; leave
            # None and already-serialized text untouched (avoids double-encoding
            # the documented ``json.dumps`` workaround).
            if value is None or isinstance(value, (str, bytes, bytearray)):
                return value
            return serializer(value)

        return process

    def literal_processor(self, dialect: Dialect) -> Callable[[Any], str] | None:
        if not self._json_write_enabled(dialect):
            return None

        def process(value: Any) -> str:
            if value is None:
                # SQL NULL, not the JSON literal 'null'.
                return "NULL"
            # Rendering the JSON as an inline SQL literal is unsafe: Snowflake
            # unescapes backslashes in single-quoted literals *before* PARSE_JSON
            # runs, so escaping cannot both preserve the data and prevent a
            # crafted value from breaking out of the literal (SQL injection).
            # The bound-parameter path (the default) is correct and safe, so
            # refuse to inline instead of emitting an unsafe literal.
            raise NotImplementedError(
                "Rendering a semi-structured JSON value as a SQL literal is not "
                "supported with enable_structured_type_json; execute the "
                "statement with bound parameters (the default) instead of "
                "literal_binds."
            )

        return process

    def bind_expression(self, bindvalue: Any) -> Any:
        # Wrap so the compiler renders PARSE_JSON(:p) on Snowflake when the flag
        # is on. Skipped for typed/structured columns (see _json_write_enabled).
        if not self._is_untyped_semi_structured():
            return bindvalue
        return _ParseJSONBind(bindvalue)


class VARIANT(_SemiStructuredJSONMixin, sqltypes.Indexable, SnowflakeType):
    __visit_name__ = "VARIANT"

    # Enable subscript access (``col["key"]`` / ``col[index]``) with JSON
    # semantics; the compiler renders it as Snowflake's bracket accessor.
    comparator_factory = sqltypes.JSON.Comparator

    @property
    def python_type(self) -> type:
        return dict


class VECTOR(SnowflakeType):
    """
    VECTOR supports the Snowflake vector data type (https://docs.snowflake.com/en/sql-reference/data-types-vector).

    Attributes:
        element_type (Union[str, sqltypes.Integer, sqltypes.Float]): can be either Integer or Float. It can be specified with "INT" or "FLOAT" string literals or using SQLAlchemy sqltypes.Integer and sqltypes.Float.
        dimension (int): length of the vector (must be a positive number).
    """

    __visit_name__ = "VECTOR"
    _VALID_ELEMENT_TYPES: ClassVar[set[str]] = {"INT", "FLOAT"}

    def __init__(
        self, element_type: str | sqltypes.Integer | sqltypes.Float, dimension: int
    ) -> None:
        self.element_type = self._normalize_element_type(element_type)
        self.dimension = self._normalize_dimension(dimension)
        super().__init__()

    def _normalize_element_type(
        self, element_type: str | sqltypes.Integer | sqltypes.Float
    ) -> str:
        if not isinstance(element_type, (str, sqltypes.Integer, sqltypes.Float)):
            raise TypeError(
                f"VECTOR element type must be a string, SQLAlchemy INT or FLOAT type, got {type(element_type).__name__}."
            )

        normalized_element_type = ""
        if isinstance(element_type, str):
            normalized_element_type = element_type.strip().upper()
            if normalized_element_type not in self._VALID_ELEMENT_TYPES:
                raise ValueError(
                    f"Unsupported VECTOR element type '{element_type}'. "
                    f"Snowflake only supports {self._VALID_ELEMENT_TYPES} element types."
                )
        elif isinstance(element_type, (sqltypes.Integer, sqltypes.Float)):
            normalized_element_type = self._map_sqlalchemy_type(element_type)

        return normalized_element_type

    @staticmethod
    def _map_sqlalchemy_type(
        element_type: sqltypes.Integer | sqltypes.Float,
    ) -> str:
        if isinstance(element_type, sqltypes.Integer):
            return "INT"
        if isinstance(element_type, sqltypes.Float):
            return "FLOAT"
        raise ValueError(
            "SQLAlchemy type must be an Integer or Float for VECTOR element."
        )

    @staticmethod
    def _normalize_dimension(dimension: int) -> int:
        if not isinstance(dimension, int):
            raise TypeError(
                f"VECTOR dimension must be an integer, got {type(dimension).__name__}."
            )
        if dimension <= 0:
            raise ValueError(
                f"VECTOR dimension must be a positive integer, got {dimension}."
            )
        return dimension

    def __repr__(self) -> str:
        return f"VECTOR({self.element_type}, {self.dimension})"

    @property
    def python_type(self) -> type:
        return list


class StructuredType(_SemiStructuredJSONMixin, sqltypes.Indexable, SnowflakeType):
    # Enable subscript access (``col["key"]`` / ``col[index]``) with JSON
    # semantics on OBJECT / ARRAY / MAP; the compiler renders it as
    # Snowflake's bracket accessor.
    comparator_factory = sqltypes.JSON.Comparator

    def __init__(self, is_semi_structured: bool = False) -> None:
        self.is_semi_structured = is_semi_structured
        super().__init__()


class MAP(StructuredType):
    __visit_name__ = "MAP"

    def __init__(
        self,
        key_type: sqltypes.TypeEngine,
        value_type: sqltypes.TypeEngine,
        not_null: bool = False,
    ) -> None:
        self.key_type = key_type
        self.value_type = value_type
        self.not_null = not_null
        super().__init__()

    @property
    def python_type(self) -> type:
        return dict


class OBJECT(StructuredType):
    __visit_name__ = "OBJECT"

    def __init__(self, **items_types: TypeEngine | tuple[TypeEngine, bool]) -> None:
        """Build an OBJECT type from optional ``field=type`` specifications.

        ``is_semi_structured`` is a ``StructuredType`` constructor parameter, not
        a field. SQLAlchemy's type-copy machinery
        (``get_cls_kwargs``/``constructor_copy``) forwards base-class ``__init__``
        params into this ``**items_types`` catch-all when copying/adapting the
        type during compilation, so it is dropped here; otherwise a copied
        ``OBJECT`` would gain a bogus ``is_semi_structured`` field, flipping an
        untyped ``OBJECT`` to "typed" and breaking the semi-structured write path.
        """
        items_types.pop("is_semi_structured", None)
        normalized: dict[str, tuple[TypeEngine, bool]] = {}
        for key, value in items_types.items():
            normalized[key] = value if isinstance(value, tuple) else (value, False)

        self.items_types: dict[str, tuple[TypeEngine, bool]] = normalized
        self.is_semi_structured = len(normalized) == 0
        super().__init__()

    def adapt(self, cls: type, **kw: Any) -> Any:
        """Copy/adapt the type while preserving its field specification.

        The field spec lives in ``items_types`` (a dict), which the default
        ``constructor_copy`` cannot reconstruct — it only forwards named
        ``__init__`` params, so fields are lost on copy/adapt. The real
        ``items_types``/``is_semi_structured`` are restored here so both typed and
        untyped ``OBJECT`` survive copy/adapt (e.g. when the column type is copied
        during INSERT compilation, reflection, or ``Table.to_metadata``).
        """
        adapted: Any = super().adapt(cls, **kw)
        if isinstance(adapted, OBJECT):
            adapted.items_types = dict(self.items_types)
            adapted.is_semi_structured = self.is_semi_structured
        return adapted

    @property
    def python_type(self) -> type:
        return dict

    def __repr__(self) -> str:
        dq = '"'
        parts = []
        for key, value in self.items_types.items():
            bare = key.strip(dq)
            if bare.isidentifier() and not keyword.iskeyword(bare):
                parts.append(f"{bare}={repr(value)}")
            else:
                # Field names that are not valid Python identifiers (e.g. a
                # quoted identifier containing a space) cannot be keyword
                # arguments; render them as a dict entry so the representation
                # stays valid Python and round-trips through Alembic autogenerate.
                parts.append(f"**{{{bare!r}: {repr(value)}}}")
        return "OBJECT(%s)" % ", ".join(parts)


class ARRAY(StructuredType):
    __visit_name__ = "SNOWFLAKE_ARRAY"

    def __init__(
        self,
        value_type: sqltypes.TypeEngine | None = None,
        not_null: bool = False,
    ) -> None:
        self.value_type = value_type
        self.not_null = not_null
        super().__init__(is_semi_structured=value_type is None)

    @property
    def python_type(self) -> type:
        return list


class TIMESTAMP_TZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_TZ"

    @property
    def python_type(self) -> type:
        return datetime


class TIMESTAMP_LTZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_LTZ"

    @property
    def python_type(self) -> type:
        return datetime


class TIMESTAMP_NTZ(SnowflakeType):
    __visit_name__ = "TIMESTAMP_NTZ"

    @property
    def python_type(self) -> type:
        return datetime


class GEOGRAPHY(SnowflakeType):
    __visit_name__ = "GEOGRAPHY"

    @property
    def python_type(self) -> type:
        return str


class GEOMETRY(SnowflakeType):
    __visit_name__ = "GEOMETRY"

    @property
    def python_type(self) -> type:
        return str


class DECFLOAT(SnowflakeType):
    """Snowflake DECFLOAT type - decimal floating-point with 38 significant digits.

    DECFLOAT supports a wider range of values than FLOAT with higher precision.
    It can represent values with exponents from approximately -6000 to +6000.

    Note: DECFLOAT has restrictions:
    - Precision is fixed at 38 digits (cannot be customized)
    - Cannot be stored in VARIANT, OBJECT, or ARRAY
    - Not supported in Iceberg or Hybrid tables
    - Does NOT support special values (inf, -inf, NaN) unlike FLOAT

    Precision: The Snowflake Python connector uses Python's decimal context
    when converting DECFLOAT to Decimal. Default context precision is 28 digits,
    which truncates values. For full 38-digit precision, use the dialect parameter::

        engine = create_engine('snowflake://...?enable_decfloat=True')

    Or set manually::

        import decimal
        decimal.getcontext().prec = 38
    """

    __visit_name__ = "DECFLOAT"
    _warned_precision: ClassVar[bool] = False

    @property
    def python_type(self) -> type:
        return decimal.Decimal

    def result_processor(
        self, dialect: Dialect, coltype: object
    ) -> Callable[[Any], Any] | None:
        """Check decimal context precision and warn if it may truncate DECFLOAT values."""
        # Check if dialect has enable_decfloat configured
        decfloat_enabled = getattr(dialect, "_enable_decfloat", False)

        def process(value: Any) -> Any:
            if value is not None and not DECFLOAT._warned_precision:
                # Skip warning if dialect has DECFLOAT support enabled
                if decfloat_enabled:
                    return value

                current_prec = decimal.getcontext().prec
                if current_prec < DECFLOAT_PRECISION:
                    warnings.warn(
                        f"Python decimal context precision ({current_prec}) is less than "
                        f"DECFLOAT precision ({DECFLOAT_PRECISION}). Values may be truncated. "
                        f"Set enable_decfloat=True in connection URL or "
                        f"decimal.getcontext().prec = {DECFLOAT_PRECISION} for full precision.",
                        UserWarning,
                        stacklevel=2,
                    )
                    DECFLOAT._warned_precision = True
            return value

        return process


class _CUSTOM_Date(SnowflakeType, sqltypes.Date):
    def literal_processor(self, dialect: Dialect) -> Callable[[Any], str | None]:  # type: ignore[override]
        def process(value: date | None) -> str | None:
            if value is not None:
                return f"'{value.isoformat()}'"
            return None

        return process


class _CUSTOM_DateTime(SnowflakeType, sqltypes.DateTime):
    def __init__(self, timezone: bool = False) -> None:
        super().__init__(timezone=timezone)

    def literal_processor(self, dialect: Dialect) -> Callable[[Any], str | None]:  # type: ignore[override]
        def process(value: datetime | None) -> str | None:
            if value is not None:
                datetime_str = value.isoformat(" ", timespec="microseconds")
                return f"'{datetime_str}'"
            return None

        return process


class _CUSTOM_Time(SnowflakeType, sqltypes.Time):
    """Internal Time type for the Snowflake dialect.

    SQLAlchemy's ``Time(timezone=True)`` has no effect in this dialect because
    Snowflake's TIME data type does not support time zones
    (https://docs.snowflake.com/en/sql-reference/data-types-datetime#time).
    The column will always be compiled to plain ``TIME`` regardless of the
    ``timezone`` flag.  To store timestamps with time-zone information use
    :class:`TIMESTAMP_TZ` or ``DateTime(timezone=True)`` instead.
    """

    def literal_processor(self, dialect: Dialect) -> Callable[[Any], str | None]:  # type: ignore[override]
        def process(value: time | None) -> str | None:
            if value is not None:
                time_str = value.isoformat(timespec="microseconds")
                return f"'{time_str}'"
            return None

        return process


class _CUSTOM_Float(SnowflakeType, sqltypes.Float):
    def bind_processor(self, dialect: Dialect) -> Callable[[Any], float | str | None]:
        return _process_float


class _CUSTOM_DECIMAL(SnowflakeType, sqltypes.DECIMAL):
    def __init__(
        self,
        precision: int | None = None,
        scale: int | None = None,
        asdecimal: bool = True,
        **kw: Any,
    ) -> None:
        super().__init__(  # type: ignore[call-overload]
            precision=precision, scale=scale, asdecimal=asdecimal, **kw
        )

    @util.memoized_property
    def _type_affinity(self) -> type:
        return sqltypes.INTEGER if self.scale == 0 else sqltypes.DECIMAL
