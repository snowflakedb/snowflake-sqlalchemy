#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import re
import warnings

import pytest
import sqlalchemy.types as sqltypes
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.exc import CompileError

from snowflake.sqlalchemy import ARRAY, MAP, OBJECT, VARIANT
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


class _Dialect:
    """Minimal stand-in exposing what the write processors read."""

    def __init__(self, enabled: bool = False, serializer=None) -> None:
        self._enable_structured_type_json = enabled
        self._json_serializer = serializer


def _semi_structured_types():
    # Untyped (semi-structured) forms get the PARSE_JSON write path.
    return [VARIANT(), OBJECT(), ARRAY()]


def _structured_types():
    # Typed / structured forms (Iceberg) are intentionally excluded.
    return [
        MAP(sqltypes.VARCHAR(), sqltypes.VARCHAR()),
        OBJECT(name=sqltypes.VARCHAR()),
        ARRAY(sqltypes.VARCHAR()),
    ]


def _norm(sql: str) -> str:
    return re.sub(r"\s+", " ", str(sql)).strip()


class TestBindProcessor:
    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_off_returns_none(self, typ):
        assert typ.bind_processor(_Dialect(enabled=False)) is None

    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_on_serializes_python_objects(self, typ):
        proc = typ.bind_processor(_Dialect(enabled=True))
        assert proc is not None
        assert proc({"a": 1, "b": [2, 3]}) == '{"a": 1, "b": [2, 3]}'
        assert proc([1, 2]) == "[1, 2]"

    @pytest.mark.parametrize("typ", _semi_structured_types())
    def test_on_passthrough_none_and_str(self, typ):
        proc = typ.bind_processor(_Dialect(enabled=True))
        assert proc(None) is None
        # already-serialized JSON text is left as-is (no double encoding)
        assert proc('{"a": 1}') == '{"a": 1}'

    @pytest.mark.parametrize("typ", _structured_types())
    def test_structured_types_excluded(self, typ):
        # Typed structured columns keep their native handling; no PARSE_JSON path.
        assert typ.bind_processor(_Dialect(enabled=True)) is None

    def test_uses_dialect_json_serializer(self):
        proc = VARIANT().bind_processor(
            _Dialect(enabled=True, serializer=lambda v: "CUSTOM")
        )
        assert proc({"a": 1}) == "CUSTOM"


class TestLiteralProcessor:
    def test_off_returns_none(self):
        assert VARIANT().literal_processor(_Dialect(enabled=False)) is None

    def test_on_raises_for_literal_value(self):
        # Inlining semi-structured JSON as a SQL literal is unsafe (see the
        # literal_processor comment), so the literal path refuses non-NULL
        # values; callers must use the default bound-parameter path.
        proc = VARIANT().literal_processor(_Dialect(enabled=True))
        with pytest.raises(NotImplementedError):
            proc({"a": 1})
        with pytest.raises(NotImplementedError):
            proc("it's")
        # None still renders as SQL NULL (covered again in TestLiteralProcessorNone).
        assert proc(None) == "NULL"


class TestParseJsonRendering:
    """bind_expression renders PARSE_JSON only on Snowflake with the flag on."""

    @staticmethod
    def _table():
        meta = MetaData()
        return Table(
            "t",
            meta,
            Column("id", Integer),
            Column("v", VARIANT),
            Column("m", MAP(sqltypes.VARCHAR(), sqltypes.VARCHAR())),
        )

    def test_update_wraps_parse_json_when_on(self):
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(update(t).where(t.c.id == 1).values(v={"a": 1}).compile(dialect=on))
        assert "SET v=PARSE_JSON(%(v)s)" in sql
        assert "'{" not in sql  # value is bound, not inlined

    def test_update_no_parse_json_when_off(self):
        t = self._table()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            off = SnowflakeDialect(enable_structured_type_json=False)
        sql = _norm(update(t).where(t.c.id == 1).values(v="x").compile(dialect=off))
        assert "PARSE_JSON" not in sql

    def test_where_wraps_parse_json_when_on(self):
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(select(t.c.id).where(t.c.v == {"a": 1}).compile(dialect=on))
        assert "PARSE_JSON(%(v_1)s)" in sql
        assert "'{" not in sql

    def test_structured_map_not_wrapped_when_on(self):
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(update(t).where(t.c.id == 1).values(m="x").compile(dialect=on))
        assert "PARSE_JSON" not in sql


class TestInsertValuesToSelect:
    """INSERT into a semi-structured column becomes INSERT ... SELECT."""

    @staticmethod
    def _table():
        meta = MetaData()
        return Table(
            "t",
            meta,
            Column("id", Integer),
            Column("v", VARIANT),
        )

    def test_single_row_insert_renders_select(self):
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(insert(t).values(id=1, v={"a": 1}).compile(dialect=on))
        assert "SELECT" in sql
        assert "PARSE_JSON(%(v)s)" in sql
        assert "VALUES" not in sql

    def test_multi_row_insert_renders_union_all(self):
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(
            insert(t)
            .values([{"id": 1, "v": {"a": 1}}, {"id": 2, "v": {"a": 2}}])
            .compile(dialect=on)
        )
        assert "UNION ALL" in sql
        assert sql.count("SELECT") >= 2
        assert sql.count("PARSE_JSON(") == 2
        assert "VALUES" not in sql

    def test_literal_binds_insert_raises(self):
        # Option 2: inlining a semi-structured value as a literal is refused;
        # compiling such a statement with literal_binds raises (SQLAlchemy wraps
        # the type's refusal in a CompileError) rather than emitting an unsafe
        # literal.
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        with pytest.raises(CompileError):
            insert(t).values(id=1, v={"a": 1}).compile(
                dialect=on, compile_kwargs={"literal_binds": True}
            )

    def test_flag_off_uses_values(self):
        t = self._table()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            off = SnowflakeDialect(enable_structured_type_json=False)
        sql = _norm(insert(t).values(id=1, v="x").compile(dialect=off))
        assert "VALUES" in sql
        assert "PARSE_JSON" not in sql

    def test_non_semi_structured_target_uses_values(self):
        # Flag on, but no semi-structured column written -> untouched VALUES path.
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(
            insert(t)
            .values(id=1)
            .compile(dialect=on, compile_kwargs={"literal_binds": True})
        )
        assert "VALUES" in sql
        assert "SELECT" not in sql
        assert "PARSE_JSON" not in sql

    def test_from_select_unaffected(self):
        # INSERT ... SELECT already valid; base compiler handles it.
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sel = select(t.c.id, t.c.v).where(t.c.id == 1)
        sql = _norm(insert(t).from_select(["id", "v"], sel).compile(dialect=on))
        assert "SELECT" in sql
        assert "VALUES" not in sql
        assert "PARSE_JSON" not in sql


class TestQmarkInsertToSelect:
    """Positional (qmark) paramstyle must render the INSERT...SELECT path too.

    Under qmark the VALUES/SELECT binds are counted positionally
    (``visited_bindparam``); this locks in that the PARSE_JSON-wrapped value
    keeps a positional placeholder and the param order/count is preserved.
    """

    @staticmethod
    def _table():
        meta = MetaData()
        return Table("t", meta, Column("id", Integer), Column("v", VARIANT))

    @staticmethod
    def _qmark_dialect():
        return SnowflakeDialect(enable_structured_type_json=True, paramstyle="qmark")

    def test_single_row_qmark_renders_positional_select(self):
        t = self._table()
        on = self._qmark_dialect()
        assert on.positional is True
        compiled = insert(t).values(id=1, v={"a": 1}).compile(dialect=on)
        sql = _norm(compiled)
        assert "SELECT ?, PARSE_JSON(?)" in sql
        assert "VALUES" not in sql
        # two positional binds, in column order
        assert list(compiled.positiontup) == ["id", "v"]

    def test_multi_row_qmark_renders_union_all_positional(self):
        t = self._table()
        on = self._qmark_dialect()
        compiled = (
            insert(t)
            .values([{"id": 1, "v": {"a": 1}}, {"id": 2, "v": {"a": 2}}])
            .compile(dialect=on)
        )
        sql = _norm(compiled)
        assert "UNION ALL" in sql
        assert sql.count("PARSE_JSON(?)") == 2
        assert "VALUES" not in sql
        # one positional bind per column per row, preserving order
        assert len(compiled.positiontup) == 4


class TestNestedInsertCompileState:
    """A nested (non-toplevel) source must not disturb the INSERT rendering.

    ``visit_insert`` only assigns ``self.dml_compile_state`` at the top level; a
    scalar subquery among the inserted values pushes onto the compiler stack, so
    this guards that the outer INSERT...SELECT still renders and the
    PARSE_JSON-wrapped column is preserved (no compile-state reuse/leak).
    """

    def test_scalar_subquery_value_insert_renders_select(self):
        meta = MetaData()
        t = Table("t", meta, Column("id", Integer), Column("v", VARIANT))
        src = Table("src", meta, Column("n", Integer))
        on = SnowflakeDialect(enable_structured_type_json=True)
        sub = select(func.max(src.c.n)).scalar_subquery()
        sql = _norm(insert(t).values(id=sub, v={"a": 1}).compile(dialect=on))
        assert sql.startswith("INSERT INTO t (id, v) SELECT")
        assert "(SELECT max(src.n)" in sql  # nested subquery rendered inline
        assert "PARSE_JSON(%(v)s)" in sql
        assert "VALUES" not in sql


class TestLiteralProcessorNone:
    """Review fix: literal_processor must emit SQL NULL, not JSON 'null'."""

    def test_none_renders_sql_null(self):
        proc = VARIANT().literal_processor(_Dialect(enabled=True))
        assert proc(None) == "NULL"

    def test_none_value_insert_is_sql_null(self):
        meta = MetaData()
        t = Table("t", meta, Column("id", Integer), Column("v", VARIANT))
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(
            insert(t)
            .values(id=1, v=None)
            .compile(dialect=on, compile_kwargs={"literal_binds": True})
        )
        assert "'null'" not in sql
        assert "NULL" in sql


class TestParseJsonBindCaching:
    """Review fix: statements binding semi-structured values must stay cacheable.

    Without a cache-key traversal on _ParseJSONBind the whole statement falls
    back to NO_CACHE (recompiled every call), which regresses even the flag-off
    path since bind_expression wraps unconditionally.
    """

    @staticmethod
    def _table():
        meta = MetaData()
        return Table("t", meta, Column("id", Integer), Column("v", VARIANT))

    def test_insert_remains_cacheable(self):
        t = self._table()
        key = insert(t).values(id=1, v={"a": 1})._generate_cache_key()
        assert key is not None  # None => uncacheable (NO_CACHE) => regression

    def test_where_getitem_remains_cacheable(self):
        t = self._table()
        key = select(t.c.id).where(t.c.v == {"a": 1})._generate_cache_key()
        assert key is not None

    def test_identical_statements_share_cache_key(self):
        t = self._table()
        k1 = insert(t).values(id=1, v={"a": 1})._generate_cache_key()
        k2 = insert(t).values(id=1, v={"a": 1})._generate_cache_key()
        assert k1 is not None and k2 is not None
        assert k1.key == k2.key


class TestPrivateCrudApiContract:
    """The INSERT...SELECT rewrite (``_render_insert_from_values_select``) calls
    SQLAlchemy's *private* ``crud._get_crud_params``. It is stable in SQLAlchemy
    2.0 but is expected to change in 2.1 (see GH #652). These canaries fail loudly
    here — rather than at query-compile time — if the private contract we depend on
    shifts, flagging that the 2.1 upgrade needs a matching adjustment.
    """

    def test_get_crud_params_exists(self):
        from sqlalchemy.sql import crud

        assert hasattr(crud, "_get_crud_params")

    def test_get_crud_params_signature_contract(self):
        # We call it as _get_crud_params(compiler, stmt, compile_state, toplevel,
        # visited_bindparam=..., **kw): four leading positionals plus **kw
        # (visited_bindparam and friends are forwarded through **kw).
        import inspect

        from sqlalchemy.sql import crud

        params = list(inspect.signature(crud._get_crud_params).parameters.values())
        positional = [
            p.name
            for p in params
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        assert positional[:4] == ["compiler", "stmt", "compile_state", "toplevel"]
        assert any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params), (
            "expected **kw to forward visited_bindparam"
        )

    def test_crud_params_result_shape(self):
        # We destructure .single_params and .all_multi_params off the result.
        from sqlalchemy.sql import crud

        fields = getattr(crud._CrudParams, "_fields", ())
        assert "single_params" in fields
        assert "all_multi_params" in fields


class TestObjectColumnCopyInsert:
    """Regression for SNOW-177555: inserting a Python ``dict`` into an untyped
    ``OBJECT`` column must produce ``INSERT ... SELECT PARSE_JSON(...)``.

    SQLAlchemy copies a column's type during INSERT compilation. The default
    copy could not reconstruct ``OBJECT`` (its field spec lives in the
    ``items_types`` dict) and leaked the base ``is_semi_structured`` param into
    that dict, flipping an untyped ``OBJECT()`` to "typed" and silently
    disabling the PARSE_JSON write path (the exact customer failure).
    """

    @staticmethod
    def _table():
        meta = MetaData()
        return Table(
            "annotation",
            meta,
            Column("id", Integer),
            Column("meta_data", OBJECT),
        )

    def test_object_insert_literal_binds_raises(self):
        # The customer's OBJECT column with literal_binds must refuse to inline
        # (Option 2); bound-param execution is covered below.
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        with pytest.raises(CompileError):
            insert(t).values(
                id=1, meta_data={"x_val": "2020-07-05", "load_type": 1}
            ).compile(dialect=on, compile_kwargs={"literal_binds": True})

    def test_object_insert_wraps_parse_json_bound(self):
        t = self._table()
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(insert(t).values(id=1, meta_data={"a": 1}).compile(dialect=on))
        # The bound value must be wrapped, not passed as a raw dict bind (the
        # original "Binding data in type (dict) is not supported" failure).
        assert "PARSE_JSON(%(meta_data)s)" in sql
        assert "VALUES" not in sql

    def test_array_insert_wraps_parse_json(self):
        meta = MetaData()
        t = Table("t", meta, Column("id", Integer), Column("a", ARRAY))
        on = SnowflakeDialect(enable_structured_type_json=True)
        sql = _norm(insert(t).values(id=1, a=[1, 2, 3]).compile(dialect=on))
        assert "PARSE_JSON(%(a)s)" in sql and "VALUES" not in sql


class TestObjectTypeCopyInvariance:
    """``OBJECT.copy()``/``adapt()`` must preserve the type faithfully."""

    def test_untyped_object_stays_untyped_after_copy(self):
        assert OBJECT()._is_untyped_semi_structured() is True
        assert OBJECT().copy()._is_untyped_semi_structured() is True
        assert OBJECT().copy().items_types == {}

    def test_typed_object_preserves_fields_after_copy(self):
        typed = OBJECT(name=String(), age=Integer())
        copied = typed.copy()
        assert copied._is_untyped_semi_structured() is False
        assert list(copied.items_types.keys()) == ["name", "age"]

    def test_object_copy_does_not_leak_is_semi_structured_field(self):
        # The base-class constructor param must never surface as a field.
        assert "is_semi_structured" not in OBJECT().copy().items_types
        assert "is_semi_structured" not in OBJECT(x=String()).copy().items_types
