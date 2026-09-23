#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
"""Unit tests for UUID and DECFLOAT inside VARIANT and structured types.

Snowflake supports storing UUID and DECFLOAT values directly in VARIANT,
ARRAY, OBJECT and MAP, so casting to VARCHAR first is no longer required.
These tests cover the three dialect-side pieces that support it:

* ``enable_native_uuid`` renders SQLAlchemy's generic ``Uuid`` as Snowflake
  ``UUID`` instead of ``CHAR(32)``.  It is opt-in because flipping it changes
  the DDL of existing ``Uuid`` / ``Mapped[uuid.UUID]`` columns.
* The structured-type compilers propagate the dialect to inner types, so a
  nested ``Uuid`` honours the flag (and Snowflake-only inner types keep
  working).
* The semi-structured JSON write path serializes ``uuid.UUID`` and
  ``decimal.Decimal`` as JSON strings, matching what Snowflake itself returns
  for ``OBJECT_CONSTRUCT('k', <value>::UUID)`` / ``::DECFLOAT``.
"""

from __future__ import annotations

import decimal
import json
import uuid

import pytest
from sqlalchemy import Column, MetaData, Table, cast, select
from sqlalchemy import types as sqltypes
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.sqltypes import UUID, Uuid

from snowflake.sqlalchemy import snowdialect
from snowflake.sqlalchemy.custom_types import (
    ARRAY,
    DECFLOAT,
    MAP,
    OBJECT,
    VARIANT,
)
from snowflake.sqlalchemy.parser.custom_type_parser import parse_type
from tests.util import random_string

# A UUID and a 38-digit DECFLOAT, matching the documented Snowflake examples.
SAMPLE_UUID_STR = "c73d9175-0a1d-48c6-8d30-df165461328b"
SAMPLE_UUID = uuid.UUID(SAMPLE_UUID_STR)
DECFLOAT_38 = decimal.Decimal("1.2345678901234567890123456789012345678")


@pytest.fixture(scope="session", autouse=True)
def _fixed_decimal_precision():
    """DECFLOAT payloads need the full 38-digit decimal context."""
    previous = decimal.getcontext().prec
    decimal.getcontext().prec = 38
    yield
    decimal.getcontext().prec = previous


def _dialect(*, native_uuid: bool = False):
    return snowdialect.dialect(enable_native_uuid=native_uuid)


def _render(type_, *, native_uuid: bool = False) -> str:
    """Render a column type through a real CreateTable compilation."""
    table = Table("t", MetaData(), Column("c", type_))
    ddl = str(CreateTable(table).compile(dialect=_dialect(native_uuid=native_uuid)))
    return " ".join(ddl.split())


class TestEnableNativeUuidFlag:
    """The opt-in flag that turns on native UUID rendering."""

    def test_flag_defaults_to_false(self):
        assert snowdialect.dialect()._enable_native_uuid is False

    def test_flag_can_be_enabled(self):
        assert _dialect(native_uuid=True)._enable_native_uuid is True

    @pytest.mark.parametrize("enabled", [True, False])
    def test_supports_native_uuid_follows_flag(self, enabled):
        """SQLAlchemy's type compiler keys off ``supports_native_uuid``."""
        assert _dialect(native_uuid=enabled).supports_native_uuid is enabled

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("true", True), ("1", True), ("false", False), ("0", False)],
    )
    def test_flag_from_url_query_parameter(self, raw, expected):
        from sqlalchemy.engine.url import make_url

        dialect = snowdialect.dialect()
        url = make_url(f"snowflake://u:p@acct/db/sch?enable_native_uuid={raw}")
        dialect.create_connect_args(url)
        assert dialect._enable_native_uuid is expected

    def test_url_parameter_is_not_forwarded_to_connector(self):
        """The flag is dialect-side only; it must not reach the connector."""
        from sqlalchemy.engine.url import make_url

        dialect = snowdialect.dialect()
        url = make_url("snowflake://u:p@acct/db/sch?enable_native_uuid=true")
        _, opts = dialect.create_connect_args(url)
        assert "enable_native_uuid" not in opts


class TestScalarUuidRendering:
    """Top-level ``Uuid`` / ``UUID`` column DDL."""

    def test_generic_uuid_is_char32_by_default(self):
        """Default behaviour is preserved: no BCR for existing CHAR(32) tables."""
        assert "c CHAR(32)" in _render(Uuid())

    def test_generic_uuid_is_native_when_enabled(self):
        assert "c UUID" in _render(Uuid(), native_uuid=True)

    @pytest.mark.parametrize("enabled", [True, False])
    def test_explicit_uppercase_uuid_is_always_native(self, enabled):
        """``sqltypes.UUID`` already means "native"; the flag must not affect it."""
        assert "c UUID" in _render(UUID(), native_uuid=enabled)

    def test_orm_annotation_is_char32_by_default(self):
        class Base(DeclarativeBase):
            pass

        class Model(Base):
            __tablename__ = "m"
            id: Mapped[int] = mapped_column(primary_key=True)
            u: Mapped[uuid.UUID]

        assert "u CHAR(32)" in " ".join(
            str(CreateTable(Model.__table__).compile(dialect=_dialect())).split()
        )

    def test_orm_annotation_is_native_when_enabled(self):
        class Base(DeclarativeBase):
            pass

        class Model(Base):
            __tablename__ = "m2"
            id: Mapped[int] = mapped_column(primary_key=True)
            u: Mapped[uuid.UUID]

        rendered = " ".join(
            str(
                CreateTable(Model.__table__).compile(dialect=_dialect(native_uuid=True))
            ).split()
        )
        assert "u UUID" in rendered


class TestNestedUuidRendering:
    """Structured-type compilers must propagate the dialect to inner types."""

    def test_array_of_generic_uuid_is_native_when_enabled(self):
        assert "c ARRAY(UUID)" in _render(ARRAY(Uuid()), native_uuid=True)

    def test_array_of_generic_uuid_is_char32_by_default(self):
        assert "c ARRAY(CHAR(32))" in _render(ARRAY(Uuid()))

    def test_map_value_of_generic_uuid_is_native_when_enabled(self):
        rendered = _render(MAP(sqltypes.VARCHAR(), Uuid()), native_uuid=True)
        assert "c MAP(VARCHAR, UUID)" in rendered

    def test_map_key_of_generic_uuid_is_native_when_enabled(self):
        rendered = _render(MAP(Uuid(), sqltypes.VARCHAR()), native_uuid=True)
        assert "c MAP(UUID, VARCHAR)" in rendered

    def test_object_field_of_generic_uuid_is_native_when_enabled(self):
        assert "c OBJECT(u UUID)" in _render(OBJECT(u=Uuid()), native_uuid=True)

    def test_nested_array_of_generic_uuid_is_native_when_enabled(self):
        rendered = _render(ARRAY(ARRAY(Uuid())), native_uuid=True)
        assert "c ARRAY(ARRAY(UUID))" in rendered

    def test_not_null_element_is_preserved(self):
        rendered = _render(ARRAY(Uuid(), not_null=True), native_uuid=True)
        assert "c ARRAY(UUID NOT NULL)" in rendered

    @pytest.mark.parametrize("enabled", [True, False])
    def test_array_of_uppercase_uuid_is_always_native(self, enabled):
        assert "c ARRAY(UUID)" in _render(ARRAY(UUID()), native_uuid=enabled)


class TestNestedSnowflakeTypesStillCompile:
    """Regression guard: dialect propagation must not break Snowflake-only types."""

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [
            (ARRAY(DECFLOAT()), "c ARRAY(DECFLOAT)"),
            (MAP(sqltypes.VARCHAR(), DECFLOAT()), "c MAP(VARCHAR, DECFLOAT)"),
            (OBJECT(amt=DECFLOAT()), "c OBJECT(amt DECFLOAT)"),
            (ARRAY(sqltypes.VARCHAR(16)), "c ARRAY(VARCHAR(16))"),
            (OBJECT(ts=DECFLOAT(), name=sqltypes.VARCHAR()), "c OBJECT(ts DECFLOAT"),
        ],
    )
    def test_snowflake_inner_types_compile(self, type_, expected):
        assert expected in _render(type_)

    def test_untyped_structured_types_unchanged(self):
        assert "c ARRAY" in _render(ARRAY())
        assert "c OBJECT" in _render(OBJECT())
        assert "c VARIANT" in _render(VARIANT())


class TestSemiStructuredJsonWrite:
    """``uuid.UUID`` and ``decimal.Decimal`` must survive the JSON write path."""

    def _bind(self, value, *, native_uuid: bool = False):
        dialect = _dialect(native_uuid=native_uuid)
        processor = VARIANT().bind_processor(dialect)
        assert processor is not None, "JSON write path should be enabled by default"
        return processor(value)

    def test_decimal_is_serialized_as_json_string(self):
        assert self._bind({"amt": decimal.Decimal("1.5")}) == '{"amt": "1.5"}'

    def test_decimal_preserves_all_38_digits(self):
        """A JSON number would round through float64 and lose digits."""
        result = json.loads(self._bind({"amt": DECFLOAT_38}))
        assert result["amt"] == str(DECFLOAT_38)
        assert decimal.Decimal(result["amt"]) == DECFLOAT_38

    def test_decimal_is_not_gated_by_native_uuid_flag(self):
        assert self._bind({"amt": decimal.Decimal("1.5")}) == '{"amt": "1.5"}'

    def test_uuid_is_serialized_as_json_string_when_enabled(self):
        result = self._bind({"user_id": SAMPLE_UUID}, native_uuid=True)
        assert json.loads(result) == {"user_id": SAMPLE_UUID_STR}

    def test_uuid_still_raises_by_default(self):
        """Default behaviour is unchanged while the flag is off."""
        with pytest.raises(TypeError, match="UUID"):
            self._bind({"user_id": SAMPLE_UUID})

    def test_uuid_in_list_is_serialized_when_enabled(self):
        result = self._bind([SAMPLE_UUID], native_uuid=True)
        assert json.loads(result) == [SAMPLE_UUID_STR]

    def test_deeply_nested_mixed_payload(self):
        payload = {"outer": [{"user_id": SAMPLE_UUID, "amt": DECFLOAT_38}]}
        result = json.loads(self._bind(payload, native_uuid=True))
        assert result["outer"][0]["user_id"] == SAMPLE_UUID_STR
        assert result["outer"][0]["amt"] == str(DECFLOAT_38)

    def test_plain_payload_unchanged(self):
        assert self._bind({"a": 1, "b": "x"}) == '{"a": 1, "b": "x"}'

    def test_already_serialized_text_untouched(self):
        assert self._bind('{"a": 1}') == '{"a": 1}'

    def test_none_untouched(self):
        assert self._bind(None) is None

    def test_unsupported_object_still_raises(self):
        class Weird:
            pass

        with pytest.raises(TypeError):
            self._bind({"k": Weird()}, native_uuid=True)

    def test_custom_json_serializer_takes_precedence(self):
        """An explicit ``json_serializer`` must not be overridden by the default."""
        dialect = snowdialect.dialect(
            enable_native_uuid=True,
            json_serializer=lambda obj: "SENTINEL",
        )
        assert VARIANT().bind_processor(dialect)({"a": 1}) == "SENTINEL"

    @pytest.mark.parametrize(
        "type_",
        [ARRAY(UUID()), MAP(sqltypes.VARCHAR(), UUID()), OBJECT(u=UUID())],
    )
    def test_typed_structured_columns_keep_native_handling(self, type_):
        """Typed columns bypass the JSON path and go straight to the connector."""
        assert type_.bind_processor(_dialect(native_uuid=True)) is None


class TestReflection:
    """``DESC TABLE`` type strings for nested UUID / DECFLOAT."""

    @pytest.mark.parametrize(
        ("type_text", "outer", "inner_attr", "inner_cls"),
        [
            ("ARRAY(UUID)", ARRAY, "value_type", Uuid),
            ("ARRAY(DECFLOAT)", ARRAY, "value_type", DECFLOAT),
            ("MAP(VARCHAR, UUID)", MAP, "value_type", Uuid),
            ("MAP(VARCHAR, DECFLOAT)", MAP, "value_type", DECFLOAT),
        ],
    )
    def test_nested_types_reflect(self, type_text, outer, inner_attr, inner_cls):
        parsed = parse_type(type_text)
        assert isinstance(parsed, outer)
        assert isinstance(getattr(parsed, inner_attr), inner_cls)

    def test_object_fields_reflect(self):
        parsed = parse_type("OBJECT(user_id UUID, amt DECFLOAT)")
        assert isinstance(parsed, OBJECT)
        assert isinstance(parsed.items_types["user_id"][0], Uuid)
        assert isinstance(parsed.items_types["amt"][0], DECFLOAT)

    def test_not_null_element_reflects(self):
        parsed = parse_type("ARRAY(UUID NOT NULL)")
        assert isinstance(parsed, ARRAY)
        assert parsed.not_null is True

    def test_reflected_uuid_round_trips_to_native_ddl(self):
        """A reflected ARRAY(UUID) must re-render as ARRAY(UUID), not CHAR(32)."""
        parsed = parse_type("ARRAY(UUID)")
        assert "c ARRAY(UUID)" in _render(parsed, native_uuid=True)


class TestReadPath:
    """Casting values back out of a VARIANT."""

    @pytest.mark.parametrize(
        ("type_", "expected"),
        [(UUID(), "AS UUID)"), (DECFLOAT(), "AS DECFLOAT)")],
    )
    def test_cast_out_of_variant(self, type_, expected):
        table = Table("events", MetaData(), Column("metadata", VARIANT()))
        stmt = select(cast(table.c.metadata["user_id"], type_))
        assert expected in str(stmt.compile(dialect=_dialect()))

    def test_generic_uuid_cast_is_native_when_enabled(self):
        table = Table("events", MetaData(), Column("metadata", VARIANT()))
        stmt = select(cast(table.c.metadata["user_id"], Uuid()))
        compiled = str(stmt.compile(dialect=_dialect(native_uuid=True)))
        assert "AS UUID)" in compiled


@pytest.fixture()
def native_uuid_engine(request):
    """An engine with ``enable_native_uuid=True``."""
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    from tests.conftest import url_factory

    engine = create_engine(
        url_factory(enable_native_uuid=True),
        poolclass=NullPool,
        future=True,
        connect_args={"disable_ocsp_checks": True, "insecure_mode": True},
    )
    request.addfinalizer(engine.dispose)
    return engine


class TestIntegration:
    """Round-trips against a real Snowflake account."""

    def test_ddl_and_reflection_round_trip(self, engine_testaccount):
        """Types created via the dialect reflect back to equivalent types."""
        table_name = "test_uuid_variant_" + random_string(5)
        metadata = MetaData()
        Table(
            table_name,
            metadata,
            Column("ids", ARRAY(UUID())),
            Column("amounts", ARRAY(DECFLOAT())),
            Column("by_name", MAP(sqltypes.VARCHAR(16), UUID())),
            Column("payload", OBJECT(user_id=UUID(), amt=DECFLOAT())),
            Column("meta", VARIANT()),
        )
        metadata.create_all(engine_testaccount)
        try:
            reflected = Table(table_name, MetaData(), autoload_with=engine_testaccount)
            assert isinstance(reflected.c.ids.type, ARRAY)
            assert isinstance(reflected.c.ids.type.value_type, Uuid)
            assert isinstance(reflected.c.amounts.type.value_type, DECFLOAT)
            assert isinstance(reflected.c.by_name.type, MAP)
            assert isinstance(reflected.c.by_name.type.value_type, Uuid)
            assert isinstance(reflected.c.payload.type, OBJECT)
            assert isinstance(reflected.c.payload.type.items_types["user_id"][0], Uuid)
            assert isinstance(reflected.c.payload.type.items_types["amt"][0], DECFLOAT)
            assert isinstance(reflected.c.meta.type, VARIANT)
        finally:
            metadata.drop_all(engine_testaccount)

    def test_scalar_uuid_column_is_native(self, native_uuid_engine):
        """``Uuid()`` creates a real Snowflake UUID column when the flag is on."""
        table_name = "test_native_uuid_" + random_string(5)
        metadata = MetaData()
        Table(table_name, metadata, Column("u", Uuid()))
        metadata.create_all(native_uuid_engine)
        try:
            with native_uuid_engine.connect() as conn:
                rows = list(conn.exec_driver_sql(f"DESC TABLE {table_name}"))
            assert rows[0][1] == "UUID", f"expected native UUID, got {rows[0][1]}"
        finally:
            metadata.drop_all(native_uuid_engine)

    def test_uuid_and_decfloat_roundtrip_through_variant(self, native_uuid_engine):
        """A ``uuid.UUID`` / ``Decimal`` payload survives a VARIANT round trip."""
        table_name = "test_variant_rt_" + random_string(5)
        metadata = MetaData()
        table = Table(table_name, metadata, Column("meta", VARIANT()))
        metadata.create_all(native_uuid_engine)
        try:
            with native_uuid_engine.begin() as conn:
                conn.execute(
                    table.insert(),
                    [{"meta": {"user_id": SAMPLE_UUID, "amt": DECFLOAT_38}}],
                )
            with native_uuid_engine.connect() as conn:
                uuid_out, amt_out = conn.exec_driver_sql(
                    # A UUID casts straight out of a VARIANT, but a DECFLOAT must
                    # go via VARCHAR: PARSE_JSON stores the value as JSON text and
                    # Snowflake rejects a direct TEXT -> DECFLOAT cast.
                    f"SELECT meta:user_id::UUID, meta:amt::VARCHAR::DECFLOAT "
                    f"FROM {table_name}"
                ).fetchone()
            assert str(uuid_out) == SAMPLE_UUID_STR
            # All 38 DECFLOAT digits survive; a JSON number would have rounded.
            assert amt_out == DECFLOAT_38
        finally:
            metadata.drop_all(native_uuid_engine)

    def test_typed_structured_columns_roundtrip(self, engine_testaccount):
        """``ARRAY(UUID)`` and ``OBJECT(... DECFLOAT)`` accept and return values."""
        table_name = "test_structured_rt_" + random_string(5)
        metadata = MetaData()
        Table(
            table_name,
            metadata,
            Column("ids", ARRAY(UUID())),
            Column("payload", OBJECT(user_id=UUID(), amt=DECFLOAT())),
        )
        metadata.create_all(engine_testaccount)
        try:
            with engine_testaccount.begin() as conn:
                conn.exec_driver_sql(
                    f"""
                    INSERT INTO {table_name}
                    SELECT ['{SAMPLE_UUID_STR}'::UUID]::ARRAY(UUID),
                           {{'user_id': '{SAMPLE_UUID_STR}'::UUID,
                             'amt': '{DECFLOAT_38}'::DECFLOAT
                            }}::OBJECT(user_id UUID, amt DECFLOAT)
                    """
                )
            with engine_testaccount.connect() as conn:
                uuid_out, amt_out = conn.exec_driver_sql(
                    f"SELECT ids[0]::UUID, payload:amt::DECFLOAT FROM {table_name}"
                ).fetchone()
            assert str(uuid_out) == SAMPLE_UUID_STR
            assert amt_out == DECFLOAT_38
        finally:
            metadata.drop_all(engine_testaccount)
