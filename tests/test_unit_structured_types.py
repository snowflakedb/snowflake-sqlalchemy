#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import ast

import pytest
from sqlalchemy.sql.sqltypes import VARCHAR, Float, Integer, Text

from snowflake.sqlalchemy import NUMBER
from snowflake.sqlalchemy.custom_types import MAP, OBJECT, TEXT, VECTOR
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from src.snowflake.sqlalchemy.parser.custom_type_parser import (
    ischema_names,
    parse_type,
    tokenize_parameters,
)


def test_compile_map_with_not_null(snapshot):
    user_table = MAP(NUMBER(10, 0), TEXT(), not_null=True)
    assert user_table.compile() == snapshot


def test_extract_parameters():
    example = "a, b(c, d, f), d"
    assert tokenize_parameters(example) == ["a", "b(c, d, f)", "d"]


@pytest.mark.parametrize(
    "input_type, expected_type",
    [
        ("BIGINT", "BIGINT"),
        ("BINARY(16)", "BINARY(16)"),
        ("BOOLEAN", "BOOLEAN"),
        ("CHAR(5)", "CHAR(5)"),
        ("CHARACTER(5)", "CHAR(5)"),
        ("DATE", "DATE"),
        ("DATETIME(3)", "DATETIME"),
        ("DECIMAL(10, 2)", "DECIMAL(10, 2)"),
        ("DEC(10, 2)", "DECIMAL(10, 2)"),
        ("DOUBLE", "FLOAT"),
        ("FLOAT", "FLOAT"),
        ("FIXED(10, 2)", "DECIMAL(10, 2)"),
        ("INT", "INTEGER"),
        ("INTEGER", "INTEGER"),
        ("NUMBER(12, 4)", "DECIMAL(12, 4)"),
        ("REAL", "REAL"),
        ("BYTEINT", "SMALLINT"),
        ("SMALLINT", "SMALLINT"),
        ("STRING(255)", "VARCHAR(255)"),
        ("TEXT(255)", "VARCHAR(255)"),
        ("VARCHAR(255)", "VARCHAR(255)"),
        ("TIME(6)", "TIME"),
        ("TIMESTAMP(3)", "TIMESTAMP"),
        ("TIMESTAMP_TZ(3)", "TIMESTAMP_TZ"),
        ("TIMESTAMP_LTZ(3)", "TIMESTAMP_LTZ"),
        ("TIMESTAMP_NTZ(3)", "TIMESTAMP_NTZ"),
        ("TINYINT", "SMALLINT"),
        ("VARBINARY(16)", "BINARY(16)"),
        ("VARCHAR(255)", "VARCHAR(255)"),
        ("VARIANT", "VARIANT"),
        (
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
        ),
        (
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR))",
            "MAP(DECIMAL(10, 0), MAP(DECIMAL(10, 0), VARCHAR))",
        ),
        ("MAP(DECIMAL(10, 0), VARIANT)", "MAP(DECIMAL(10, 0), VARIANT)"),
        ("OBJECT", "OBJECT"),
        (
            "OBJECT(a DECIMAL(10, 0) NOT NULL, b DECIMAL(10, 0), c VARCHAR NOT NULL)",
            "OBJECT(a DECIMAL(10, 0) NOT NULL, b DECIMAL(10, 0), c VARCHAR NOT NULL)",
        ),
        ("ARRAY", "ARRAY"),
        (
            "ARRAY(MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
            "ARRAY(MAP(DECIMAL(10, 0), VARCHAR NOT NULL))",
        ),
        ("GEOGRAPHY", "GEOGRAPHY"),
        ("GEOMETRY", "GEOMETRY"),
        ("VECTOR(FLOAT, 3)", "VECTOR(FLOAT, 3)"),
        ("VECTOR(INT, 256)", "VECTOR(INT, 256)"),
    ],
)
def test_snowflake_data_types(input_type, expected_type):
    assert parse_type(input_type).compile() == expected_type


def test_vector_type_accepts_string_and_case_insensitive():
    assert repr(VECTOR("FLOAT", 3)) == "VECTOR(FLOAT, 3)"
    assert repr(VECTOR("int", 256)) == "VECTOR(INT, 256)"
    assert repr(VECTOR("int", 5000)) == "VECTOR(INT, 5000)"


def test_vector_type_accepts_sqlalchemy_types():
    assert repr(VECTOR(Integer(), 8)) == "VECTOR(INT, 8)"
    assert repr(VECTOR(Float(), 2)) == "VECTOR(FLOAT, 2)"


def test_vector_invalid_dimension():
    with pytest.raises(ValueError):
        VECTOR("FLOAT", 0)
    with pytest.raises(ValueError):
        VECTOR("FLOAT", -10)
    with pytest.raises(TypeError):
        VECTOR("FLOAT", "10")


def test_vector_rejects_invalid_element_type():
    with pytest.raises(ValueError):
        VECTOR("STRING", 10)
    with pytest.raises(TypeError):
        VECTOR(Text(), 10)


# ---------------------------------------------------------------------------
# UUID type mapping (SA 2.x only)
# ---------------------------------------------------------------------------


@pytest.mark.feature_v20
def test_uuid_parse_type_roundtrip():
    """parse_type('UUID') returns a UUID instance that compiles back to 'UUID'."""
    result = parse_type("UUID")
    assert result.compile() == "UUID"


@pytest.mark.feature_v20
def test_uuid_in_ischema_names():
    """ischema_names maps 'UUID' to sqlalchemy.sql.sqltypes.UUID on SA 2.x."""
    from sqlalchemy.sql.sqltypes import UUID

    assert "UUID" in ischema_names
    assert issubclass(ischema_names["UUID"], UUID)


@pytest.mark.feature_v20
def test_get_type_kwargs_uuid_returns_as_uuid_false():
    """_get_type_kwargs returns {'as_uuid': False} for UUID so values are reflected as strings."""
    from sqlalchemy.sql.sqltypes import UUID

    dialect = SnowflakeDialect()
    result = dialect._get_type_kwargs(UUID, None, None, None)
    assert result == {"as_uuid": False}


@pytest.mark.feature_v20
def test_resolve_column_type_uuid_produces_string_uuid():
    """_resolve_column_type for 'UUID' returns UUID(as_uuid=False), not NullType."""
    from sqlalchemy.sql.sqltypes import UUID

    dialect = SnowflakeDialect()
    col_type = dialect._resolve_column_type(
        "UUID",
        character_maximum_length=None,
        numeric_precision=None,
        numeric_scale=None,
        data_type_alias="UUID",
        column_name="id",
    )
    assert isinstance(col_type, UUID)
    assert col_type.as_uuid is False


class TestTokenizeParametersQuoteAware:
    """tokenize_parameters must not split on delimiters inside double-quoted identifiers."""

    @pytest.mark.parametrize(
        "input_str, delimiter, expected",
        [
            pytest.param(
                '"foo,bar" TEXT', ",", ['"foo,bar" TEXT'], id="comma_in_quotes"
            ),
            pytest.param(
                '"field1" TEXT,"field2" NUMBER',
                ",",
                ['"field1" TEXT', '"field2" NUMBER'],
                id="comma_outside_quotes",
            ),
            pytest.param("a, b, c", ",", ["a", "b", "c"], id="plain_tokens"),
        ],
    )
    def test_tokenize(self, input_str, delimiter, expected):
        assert tokenize_parameters(input_str, delimiter) == expected


class TestObjectTypeParsing:
    """parse_type correctly handles OBJECT fields with special characters in names."""

    def test_object_with_comma_in_field_name_parses_cleanly(self):
        """OBJECT("foo,bar" TEXT) must yield exactly one field with the full name."""
        obj = parse_type('OBJECT("foo,bar" TEXT)')
        assert (
            len(obj.items_types) == 1
        ), f"Expected 1 field, got {list(obj.items_types.keys())!r}"
        assert (
            '"foo,bar"' in obj.items_types
        ), f"Expected key '\"foo,bar\"', got {list(obj.items_types.keys())!r}"

    def test_object_with_normal_fields_parses_correctly(self):
        """Baseline: OBJECT with normal field names still parses correctly."""
        obj = parse_type("OBJECT(name TEXT, age NUMBER)")
        assert set(obj.items_types.keys()) == {"name", "age"}


class TestVisitObjectKeyQuoting:
    """visit_OBJECT must wrap every key in double-quotes in the DDL output."""

    def _type_compiler(self):
        return SnowflakeDialect().type_compiler

    def test_special_char_in_key_is_quoted(self):
        """A key with ')' must be surrounded by double-quotes in DDL output."""
        obj = OBJECT(normal_field=VARCHAR(10))
        obj.items_types = {"field)injected": (VARCHAR(10), False)}
        result = self._type_compiler().process(obj)
        assert (
            '"field)injected"' in result
        ), f"Expected quoted key in DDL, got {result!r}"

    def test_already_quoted_key_is_not_double_quoted(self):
        """A key already stored with double-quotes (from parse_type) must not be re-quoted."""
        obj = parse_type('OBJECT("WeirdField" TEXT)')
        result = self._type_compiler().process(obj)
        assert '"WeirdField"' in result
        assert '""WeirdField""' not in result, f"Key was double-quoted: {result!r}"

    def test_normal_key_is_preserved(self):
        """A plain identifier key must appear in DDL (quoted only if required by dialect rules)."""
        obj = OBJECT(my_field=VARCHAR(10))
        result = self._type_compiler().process(obj)
        assert (
            "my_field" in result
        ), f"Expected key 'my_field' in DDL output, got {result!r}"

    def test_fake_quoted_key_is_re_quoted(self):
        """A key that starts and ends with '"' but contains structure must be re-quoted, not passed verbatim."""
        obj = OBJECT()
        # Simulate a key that the old startswith/endswith guard let through verbatim
        obj.items_types = {'"a"."b"': (VARCHAR(10), False)}
        result = self._type_compiler().process(obj)
        # The dot and the inner quotes must not appear unescaped in the output
        assert (
            '"a"."b"' not in result
        ), f"Value was emitted verbatim in DDL: {result!r}"


class TestObjectRepr:
    """OBJECT.__repr__ must produce valid Python for all field name forms."""

    @staticmethod
    def _try_parse_python(source: str):
        try:
            ast.parse(source)
            return True, None
        except SyntaxError as exc:
            return False, str(exc)

    @pytest.mark.parametrize(
        "obj",
        [
            pytest.param(OBJECT(my_field=VARCHAR(10)), id="plain_field"),
            pytest.param(
                parse_type('OBJECT("WeirdField" VARCHAR(10))'), id="double_quoted_field"
            ),
        ],
    )
    def test_repr_is_valid_python(self, obj):
        ok, err = self._try_parse_python(repr(obj))
        assert ok, f"repr is not valid Python: {err}\n  repr: {repr(obj)!r}"

    def test_double_quoted_field_name_stripped_in_repr(self):
        """The surrounding double-quotes are stripped so the kwarg is a valid identifier."""
        obj = parse_type('OBJECT("WeirdField" VARCHAR(10))')
        assert "WeirdField=" in repr(
            obj
        ), f"Expected 'WeirdField=' in repr, got: {repr(obj)!r}"
        assert '"WeirdField"=' not in repr(
            obj
        ), f"Double-quotes must be stripped from repr kwarg: {repr(obj)!r}"
