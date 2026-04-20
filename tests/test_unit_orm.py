#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""Unit tests for SnowflakeBase, snowflake_declarative_base, and SnowflakeSession.

No database connection required — these tests use in-memory SQLite.
"""

from unittest import mock

import pytest
from sqlalchemy import JSON, Column, Integer, String, func, text
from sqlalchemy.orm import Session, declarative_base

from snowflake.sqlalchemy.compat import IS_VERSION_20

# ---------------------------------------------------------------------------
# Helpers — model builders
# ---------------------------------------------------------------------------


def _make_model_for_base(base_cls, table_suffix=""):
    """Return a mapped model class with a variety of column types."""

    class MyModel(base_cls):
        __tablename__ = f"my_model_sf{table_suffix}"

        id = Column(Integer, primary_key=True)
        # Plain nullable — should be pre-populated with None
        name = Column(String)
        # Server default — should NOT be pre-populated
        created_at = Column(String, server_default=text("'now'"))
        # Scalar Python default — should be pre-populated with the value
        status = Column(String, default="active")
        # Callable Python default — should NOT be pre-populated
        token = Column(String, default=lambda: "generated")
        # SQL-expression default — should NOT be pre-populated
        ts = Column(String, default=func.now())
        # JSON column (should_evaluate_none=True) — should NOT be pre-populated
        metadata_json = Column("metadata_json", JSON)

    return MyModel


def _user_keys(d):
    """Return state_dict keys that are not SA internals."""
    return {k for k in d if not k.startswith("_sa")}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def snowflake_base():
    """SnowflakeBase class (SA 2.x DeclarativeBase subclass)."""
    from snowflake.sqlalchemy.orm import SnowflakeBase

    return SnowflakeBase


@pytest.fixture(scope="module")
def sf_declarative_base():
    """A base produced by snowflake_declarative_base()."""
    from snowflake.sqlalchemy.orm import snowflake_declarative_base

    return snowflake_declarative_base()


@pytest.fixture(scope="module")
def snowflake_session_cls():
    """SnowflakeSession class."""
    from snowflake.sqlalchemy.orm import SnowflakeSession

    return SnowflakeSession


# ---------------------------------------------------------------------------
# Tests: SnowflakeBase constructor (SA 2.x class-based)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not IS_VERSION_20, reason="SnowflakeBase requires SQLAlchemy 2.x")
class TestSnowflakeBaseConstructor:
    """Verify _snowflake_constructor behaviour via SnowflakeBase."""

    @pytest.fixture(scope="class")
    def MyModel(self, snowflake_base):
        return _make_model_for_base(snowflake_base, "_cls")

    def test_plain_nullable_column_prepopulated_none(self, MyModel):
        """Plain nullable column (no default) must be in state_dict as None."""
        obj = MyModel(id=1)
        assert "name" in obj.__dict__, "plain nullable column must be in state_dict"
        assert obj.__dict__["name"] is None

    def test_primary_key_not_prepopulated(self, MyModel):
        """Primary key column must NOT be pre-populated with None."""
        obj = MyModel()
        assert "id" not in obj.__dict__, "PK column must be absent from state_dict"

    def test_primary_key_user_value_preserved(self, MyModel):
        """Primary key supplied by user must appear in state_dict."""
        obj = MyModel(id=42)
        assert obj.__dict__.get("id") == 42

    def test_server_default_column_not_prepopulated(self, MyModel):
        """server_default column must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert (
            "created_at" not in obj.__dict__
        ), "server_default column must be absent from state_dict"

    def test_scalar_default_column_prepopulated_with_value(self, MyModel):
        """Scalar Python default column must be pre-populated with the scalar value."""
        obj = MyModel(id=1)
        assert "status" in obj.__dict__, "scalar-default column must be in state_dict"
        assert obj.__dict__["status"] == "active"

    def test_callable_default_column_not_prepopulated(self, MyModel):
        """Callable Python default column must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert (
            "token" not in obj.__dict__
        ), "callable-default column must be absent from state_dict"

    def test_sql_expression_default_column_not_prepopulated(self, MyModel):
        """SQL-expression default column must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert (
            "ts" not in obj.__dict__
        ), "sql-expression-default column must be absent from state_dict"

    def test_should_evaluate_none_column_not_prepopulated(self, MyModel):
        """Column with should_evaluate_none=True (e.g. JSON) must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert (
            "metadata_json" not in obj.__dict__
        ), "should_evaluate_none column must be absent from state_dict"

    def test_user_supplied_server_default_column_preserved(self, MyModel):
        """User-supplied value for a server_default column must be preserved."""
        obj = MyModel(id=1, created_at="2025-01-01")
        assert obj.__dict__["created_at"] == "2025-01-01"

    def test_user_supplied_value_preserved(self, MyModel):
        """User-supplied kwargs must appear in state_dict with the given value."""
        obj = MyModel(id=1, name="hello")
        assert obj.__dict__["name"] == "hello"

    def test_user_supplied_overrides_scalar_default(self, MyModel):
        """User value for scalar-default column must override the pre-populated default."""
        obj = MyModel(id=1, status="pending")
        assert obj.__dict__["status"] == "pending"

    def test_invalid_kwarg_raises_type_error(self, MyModel):
        """Unknown keyword arguments must raise TypeError."""
        with pytest.raises(TypeError):
            MyModel(id=1, nonexistent_column="value")

    def test_all_objects_produce_same_state_dict_keys(self, MyModel):
        """Objects with different nullable columns provided must share the same
        state_dict key set — the core bulk-batching requirement."""
        obj1 = MyModel(id=1)
        obj2 = MyModel(id=2, name="foo")
        obj3 = MyModel(id=3, status="inactive")

        keys1 = _user_keys(obj1.__dict__)
        keys2 = _user_keys(obj2.__dict__)
        keys3 = _user_keys(obj3.__dict__)
        assert keys1 == keys2 == keys3, (
            f"All objects must have the same state_dict key set: "
            f"{keys1} vs {keys2} vs {keys3}"
        )


# ---------------------------------------------------------------------------
# Tests: snowflake_declarative_base (function-based factory)
# ---------------------------------------------------------------------------


class TestSnowflakeDeclarativeBase:
    """Verify snowflake_declarative_base produces the same constructor behaviour."""

    @pytest.fixture(scope="class")
    def MyModel(self, sf_declarative_base):
        return _make_model_for_base(sf_declarative_base, "_fn")

    def test_plain_nullable_column_prepopulated_none(self, MyModel):
        """Plain nullable column must be in state_dict as None."""
        obj = MyModel(id=1)
        assert "name" in obj.__dict__
        assert obj.__dict__["name"] is None

    def test_primary_key_not_prepopulated(self, MyModel):
        """Primary key column must NOT be pre-populated."""
        obj = MyModel()
        assert "id" not in obj.__dict__

    def test_server_default_column_not_prepopulated(self, MyModel):
        """server_default column must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert "created_at" not in obj.__dict__

    def test_scalar_default_column_prepopulated(self, MyModel):
        """Scalar Python default must be pre-populated with the scalar value."""
        obj = MyModel(id=1)
        assert obj.__dict__.get("status") == "active"

    def test_callable_default_not_prepopulated(self, MyModel):
        """Callable default column must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert "token" not in obj.__dict__

    def test_sql_expression_default_not_prepopulated(self, MyModel):
        """SQL-expression default column must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert "ts" not in obj.__dict__

    def test_should_evaluate_none_column_not_prepopulated(self, MyModel):
        """Column with should_evaluate_none=True (e.g. JSON) must NOT be pre-populated."""
        obj = MyModel(id=1)
        assert "metadata_json" not in obj.__dict__

    def test_all_objects_produce_same_state_dict_keys(self, MyModel):
        """Core batching invariant: same key set regardless of provided kwargs."""
        obj1 = MyModel(id=1)
        obj2 = MyModel(id=2, name="bar")
        assert _user_keys(obj1.__dict__) == _user_keys(obj2.__dict__)

    def test_invalid_kwarg_raises_type_error(self, MyModel):
        """Unknown keyword arguments must raise TypeError."""
        with pytest.raises(TypeError):
            MyModel(id=1, nonexistent_column="value")


# ---------------------------------------------------------------------------
# Tests: SnowflakeSession.bulk_save_objects calls _bulk_save_mappings with
# render_nulls=True
# ---------------------------------------------------------------------------


class TestSnowflakeSession:
    """Verify SnowflakeSession overrides bulk_save_objects with render_nulls=True."""

    def test_bulk_save_objects_passes_render_nulls_true(self, snowflake_session_cls):
        """SnowflakeSession.bulk_save_objects must call _bulk_save_mappings
        with render_nulls=True."""
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///:memory:")
        Base = declarative_base()

        class Item(Base):
            __tablename__ = "item"

            id = Column(Integer, primary_key=True)
            name = Column(String)

        Base.metadata.create_all(engine)

        session = snowflake_session_cls(bind=engine)

        render_nulls_values = []
        original = session._bulk_save_mappings

        def capturing(*args, **kwargs):
            render_nulls_values.append(kwargs.get("render_nulls"))
            return original(*args, **kwargs)

        with mock.patch.object(session, "_bulk_save_mappings", side_effect=capturing):
            session.bulk_save_objects([Item(id=1, name="a"), Item(id=2, name="b")])

        assert render_nulls_values, "Expected at least one call to _bulk_save_mappings"
        assert all(
            v is True for v in render_nulls_values
        ), f"All calls must use render_nulls=True, got: {render_nulls_values}"

    def test_bulk_save_objects_signature_compatible(self, snowflake_session_cls):
        """SnowflakeSession.bulk_save_objects must accept the same kwargs as
        Session.bulk_save_objects."""
        import inspect

        sig = inspect.signature(snowflake_session_cls.bulk_save_objects)
        params = set(sig.parameters.keys())
        expected = {
            "self",
            "objects",
            "return_defaults",
            "update_changed_only",
            "preserve_order",
        }
        assert expected.issubset(params), f"Missing params: {expected - params}"

    def test_is_session_subclass(self, snowflake_session_cls):
        """SnowflakeSession must be a subclass of sqlalchemy.orm.Session."""
        assert issubclass(snowflake_session_cls, Session)


# ---------------------------------------------------------------------------
# Tests: SnowflakeBase + SnowflakeSession end-to-end (SA 2.x)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not IS_VERSION_20, reason="SnowflakeBase requires SQLAlchemy 2.x")
class TestSnowflakeBaseWithSessionEndToEnd:
    """Verify that SnowflakeBase + SnowflakeSession together produce uniform
    parameter-key sets for bulk_save_objects, exercising the full pipeline."""

    def test_all_objects_same_params_with_render_nulls(self, snowflake_session_cls):
        """Objects with different nullable-column populations must all produce
        the same parameter dict keys when passed through SnowflakeBase +
        SnowflakeSession.bulk_save_objects."""
        from sqlalchemy import create_engine

        from snowflake.sqlalchemy.orm import SnowflakeBase

        class Widget(SnowflakeBase):
            __tablename__ = "widget_e2e"

            id = Column(Integer, primary_key=True)
            label = Column(String)
            color = Column(String)

        engine = create_engine("sqlite:///:memory:")
        SnowflakeBase.metadata.create_all(engine)

        session = snowflake_session_cls(bind=engine)

        # Create objects with different columns supplied — all should have
        # the same state_dict keys thanks to SnowflakeBase.
        objs = [
            Widget(id=1),
            Widget(id=2, label="foo"),
            Widget(id=3, color="red"),
            Widget(id=4, label="bar", color="blue"),
        ]

        # Verify uniform key sets
        key_sets = [_user_keys(o.__dict__) for o in objs]
        assert all(
            ks == key_sets[0] for ks in key_sets
        ), f"Key sets differ across objects: {key_sets}"

        # Capture render_nulls from _bulk_save_mappings calls
        render_nulls_values = []
        original = session._bulk_save_mappings

        def capturing(*args, **kwargs):
            render_nulls_values.append(kwargs.get("render_nulls"))
            return original(*args, **kwargs)

        with mock.patch.object(session, "_bulk_save_mappings", side_effect=capturing):
            session.bulk_save_objects(objs)

        assert render_nulls_values, "Expected _bulk_save_mappings to be called"
        assert all(v is True for v in render_nulls_values)
        # Only one call expected since all objects share the same mapper
        assert (
            len(render_nulls_values) == 1
        ), f"Expected single _bulk_save_mappings call but got {len(render_nulls_values)}"

    def test_bulk_save_objects_empty_list(self, snowflake_session_cls):
        """Passing an empty list to bulk_save_objects must not raise."""
        from sqlalchemy import create_engine

        engine = create_engine("sqlite:///:memory:")
        session = snowflake_session_cls(bind=engine)
        # Should be a no-op
        session.bulk_save_objects([])


# ---------------------------------------------------------------------------
# Tests: public exports from snowflake.sqlalchemy top-level package
# ---------------------------------------------------------------------------


class TestPublicExports:
    """Verify SnowflakeBase, snowflake_declarative_base, and SnowflakeSession
    are importable from snowflake.sqlalchemy."""

    def test_snowflake_session_exported(self):
        from snowflake.sqlalchemy import SnowflakeSession  # noqa: F401

        assert SnowflakeSession is not None

    def test_snowflake_declarative_base_exported(self):
        from snowflake.sqlalchemy import snowflake_declarative_base  # noqa: F401

        assert callable(snowflake_declarative_base)

    @pytest.mark.skipif(
        not IS_VERSION_20, reason="SnowflakeBase requires SQLAlchemy 2.x"
    )
    def test_snowflake_base_exported(self):
        from snowflake.sqlalchemy import SnowflakeBase  # noqa: F401

        assert SnowflakeBase is not None

    @pytest.mark.skipif(
        not IS_VERSION_20, reason="SnowflakeBase requires SQLAlchemy 2.x"
    )
    def test_snowflake_base_in_all(self):
        """SnowflakeBase must be listed in __all__ on SA 2.x."""
        import snowflake.sqlalchemy as sf

        assert "SnowflakeBase" in sf.__all__

    def test_snowflake_session_in_all(self):
        """SnowflakeSession must be listed in __all__."""
        import snowflake.sqlalchemy as sf

        assert "SnowflakeSession" in sf.__all__

    def test_snowflake_declarative_base_in_all(self):
        """snowflake_declarative_base must be listed in __all__."""
        import snowflake.sqlalchemy as sf

        assert "snowflake_declarative_base" in sf.__all__
