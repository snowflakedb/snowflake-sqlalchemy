#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import json

import pytest
import sqlalchemy.types as sqltypes
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool, StaticPool

from snowflake.sqlalchemy import ARRAY, MAP, OBJECT, VARIANT
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


class _Dialect:
    """Minimal stand-in exposing only what ``result_processor`` reads."""

    def __init__(self, enabled: bool = False, deserializer=None) -> None:
        self._enable_structured_type_json = enabled
        self._json_deserializer = deserializer


def _semi_structured_types():
    return [
        VARIANT(),
        OBJECT(),
        ARRAY(),
        MAP(sqltypes.VARCHAR(), sqltypes.VARCHAR()),
    ]


class TestStructuredTypeJsonResultProcessor:
    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_no_processor_when_flag_off(self, typ):
        # Default-off: no processor, so values pass through untouched (no BCR).
        assert typ.result_processor(_Dialect(enabled=False), None) is None

    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_deserializes_json_string_when_flag_on(self, typ):
        proc = typ.result_processor(_Dialect(enabled=True), None)
        assert proc is not None
        assert proc('{"a": 1, "b": [1, 2]}') == {"a": 1, "b": [1, 2]}
        assert proc("[1, 2, 3]") == [1, 2, 3]

    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_deserializes_bytes_and_bytearray(self, typ):
        # The connector may hand back bytes/bytearray for semi-structured columns
        # depending on configuration; both must decode like the str form.
        proc = typ.result_processor(_Dialect(enabled=True), None)
        assert proc(b'{"a": 1, "b": [1, 2]}') == {"a": 1, "b": [1, 2]}
        assert proc(bytearray(b"[1, 2, 3]")) == [1, 2, 3]

    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_none_passes_through(self, typ):
        proc = typ.result_processor(_Dialect(enabled=True), None)
        assert proc(None) is None

    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_already_parsed_value_passes_through(self, typ):
        # If the connector already returned a parsed object, don't re-process it.
        proc = typ.result_processor(_Dialect(enabled=True), None)
        parsed_dict = {"x": 1}
        parsed_list = [1, 2]
        assert proc(parsed_dict) is parsed_dict
        assert proc(parsed_list) is parsed_list

    def test_uses_dialect_json_deserializer_when_set(self):
        sentinel = object()
        seen = []

        def deserializer(value):
            seen.append(value)
            return sentinel

        proc = VARIANT().result_processor(
            _Dialect(enabled=True, deserializer=deserializer), None
        )
        assert proc('{"a": 1}') is sentinel
        assert seen == ['{"a": 1}']

    def test_malformed_json_propagates(self):
        # Corrupt JSON surfaces as JSONDecodeError rather than being swallowed.
        proc = VARIANT().result_processor(_Dialect(enabled=True), None)
        with pytest.raises(json.JSONDecodeError):
            proc('{"a": ')


class TestEnableStructuredTypeJsonFlag:
    def test_default_is_false(self):
        assert SnowflakeDialect()._enable_structured_type_json is False

    def test_constructor_param_sets_flag(self):
        dialect = SnowflakeDialect(enable_structured_type_json=True)
        assert dialect._enable_structured_type_json is True

    def test_url_param_enables_flag(self):
        dialect = SnowflakeDialect()
        url = make_url(
            "snowflake://user:pass@account/db/schema?enable_structured_type_json=true"
        )
        dialect.create_connect_args(url)
        assert dialect._enable_structured_type_json is True

    def test_url_param_absent_keeps_default(self):
        dialect = SnowflakeDialect()
        url = make_url("snowflake://user:pass@account/db/schema")
        dialect.create_connect_args(url)
        assert dialect._enable_structured_type_json is False


class TestEnableStructuredTypeJsonFlagPersistsWithPooling:
    """The flag lives on the engine-wide (shared) dialect, so it must survive
    regardless of the connection pool — including ``NullPool``, which builds a
    fresh DBAPI connection for every checkout."""

    @pytest.mark.parametrize("poolclass", [StaticPool, NullPool])
    def test_url_flag_persists_across_pool(self, poolclass):
        engine = create_engine(
            "snowflake://user:pass@account/db/schema?enable_structured_type_json=true",
            poolclass=poolclass,
        )
        try:
            # create_engine applies the URL flag to the shared dialect.
            assert engine.dialect._enable_structured_type_json is True
            # NullPool recreates the connection each checkout; re-running the
            # connect-args parsing must keep the flag on (not reset to default).
            engine.dialect.create_connect_args(engine.url)
            assert engine.dialect._enable_structured_type_json is True
        finally:
            engine.dispose()

    @pytest.mark.parametrize("poolclass", [StaticPool, NullPool])
    def test_constructor_flag_persists_across_pool(self, poolclass):
        # The constructor-kwarg path is pool-independent: set on the shared
        # dialect at engine creation and never reset by pool churn.
        engine = create_engine(
            "snowflake://user:pass@account/db/schema",
            poolclass=poolclass,
            enable_structured_type_json=True,
        )
        try:
            assert engine.dialect._enable_structured_type_json is True
        finally:
            engine.dispose()
