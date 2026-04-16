#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import Column, Integer, String, Text, create_engine, event, text
from sqlalchemy.orm import Session

from snowflake.sqlalchemy.compat import IS_VERSION_20
from snowflake.sqlalchemy.orm import (
    SnowflakeBase,
    SnowflakeSession,
    mapper_uses_snowflake_bulk,
)

if IS_VERSION_20:
    from sqlalchemy.orm import DeclarativeBase

    class _PlainBase(DeclarativeBase):
        __abstract__ = True


if IS_VERSION_20:

    class _SfModel(SnowflakeBase):
        __tablename__ = "snowflake_orm_sf"
        id = Column(Integer, primary_key=True)
        name = Column(String)

    class _PlainModel(_PlainBase):
        __tablename__ = "snowflake_orm_plain"
        id = Column(Integer, primary_key=True)
        name = Column(String)

    class _WideBulkModel(SnowflakeBase):
        """Optional nullable columns for batching / e2e tests."""

        __tablename__ = "snowflake_orm_wide"
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        tag = Column(String, nullable=True)
        note = Column(Text, nullable=True)

    class _TaggedManualBulk(_PlainBase):
        """Opt-in via ``__snowflake_sqlalchemy_bulk__`` without ``SnowflakeBase``."""

        __tablename__ = "snowflake_orm_tagged"
        __snowflake_sqlalchemy_bulk__ = True
        id = Column(Integer, primary_key=True)
        label = Column(String)

else:
    from sqlalchemy.orm import declarative_base

    _Base = declarative_base()

    class _SfModel(SnowflakeBase, _Base):
        __tablename__ = "snowflake_orm_sf"
        id = Column(Integer, primary_key=True)
        name = Column(String)

    class _PlainModel(_Base):
        __tablename__ = "snowflake_orm_plain"
        id = Column(Integer, primary_key=True)
        name = Column(String)

    class _WideBulkModel(SnowflakeBase, _Base):
        __tablename__ = "snowflake_orm_wide"
        id = Column(Integer, primary_key=True)
        name = Column(String, nullable=False)
        tag = Column(String, nullable=True)
        note = Column(Text, nullable=True)

    class _TaggedManualBulk(_Base):
        __tablename__ = "snowflake_orm_tagged"
        __snowflake_sqlalchemy_bulk__ = True
        id = Column(Integer, primary_key=True)
        label = Column(String)


@pytest.fixture
def sqlite_engine():
    engine = create_engine("sqlite:///:memory:")
    yield engine
    engine.dispose()


def test_public_exports_from_snowflake_sqlalchemy():
    from snowflake.sqlalchemy import SnowflakeBase as SB
    from snowflake.sqlalchemy import SnowflakeSession as SS
    from snowflake.sqlalchemy import mapper_uses_snowflake_bulk as M

    assert SB is SnowflakeBase
    assert SS is SnowflakeSession
    assert M is mapper_uses_snowflake_bulk


def test_mapper_uses_snowflake_bulk_true(sqlite_engine):
    _SfModel.metadata.create_all(sqlite_engine)
    assert mapper_uses_snowflake_bulk(_SfModel) is True


def test_mapper_uses_snowflake_bulk_false(sqlite_engine):
    _PlainModel.metadata.create_all(sqlite_engine)
    assert mapper_uses_snowflake_bulk(_PlainModel) is False


def test_mapper_uses_snowflake_bulk_manual_class_flag(sqlite_engine):
    _TaggedManualBulk.metadata.create_all(sqlite_engine)
    assert mapper_uses_snowflake_bulk(_TaggedManualBulk) is True


def test_snowflake_model_invalid_keyword_raises_typeerror():
    with pytest.raises(TypeError, match="invalid keyword"):
        _SfModel(id=1, name="ok", not_a_column="bad")


def test_snowflake_model_user_supplied_values_preserved():
    row = _SfModel(id=7, name="saved")
    assert row.id == 7
    assert row.name == "saved"


def test_snowflake_session_bulk_save_objects_render_nulls_for_sf_model(sqlite_engine):
    _SfModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)
    row = _SfModel(name="a")

    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_save_objects([row])

    mock_bm.assert_called_once()
    _args, kwargs = mock_bm.call_args
    if IS_VERSION_20:
        assert kwargs.get("render_nulls") is True
    else:
        # self, mapper, states, isupdate, isstates, return_defaults,
        # update_changed_only, render_nulls
        assert _args[7] is True


def test_snowflake_session_bulk_save_objects_render_nulls_false_for_plain(
    sqlite_engine,
):
    _PlainModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)
    row = _PlainModel(name="b")

    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_save_objects([row])

    mock_bm.assert_called_once()
    _args, kwargs = mock_bm.call_args
    if IS_VERSION_20:
        assert kwargs.get("render_nulls") is False
    else:
        assert _args[7] is False


def test_snowflake_session_bulk_insert_mappings_auto_render_nulls(sqlite_engine):
    _SfModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)

    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_insert_mappings(_SfModel, [{"id": 1, "name": "x"}])

    mock_bm.assert_called_once()
    _args, kwargs = mock_bm.call_args
    if IS_VERSION_20:
        assert kwargs.get("render_nulls") is True
    else:
        # self, mapper, states, isupdate, isstates, return_defaults,
        # update_changed_only, render_nulls
        assert _args[7] is True


def test_snowflake_session_bulk_insert_mappings_explicit_render_nulls_false(
    sqlite_engine,
):
    _SfModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)

    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_insert_mappings(
            _SfModel, [{"id": 1, "name": "x"}], render_nulls=False
        )

    mock_bm.assert_called_once()
    _args, kwargs = mock_bm.call_args
    if IS_VERSION_20:
        assert kwargs.get("render_nulls") is False
    else:
        assert _args[7] is False


def test_bulk_save_objects_empty_list_does_not_call_bulk_mappings(sqlite_engine):
    _SfModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)
    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_save_objects([])
    mock_bm.assert_not_called()


def test_bulk_save_objects_mixed_sf_and_plain_two_bulk_calls(sqlite_engine):
    _SfModel.metadata.create_all(sqlite_engine)
    _PlainModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)

    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_save_objects(
            [
                _SfModel(id=1, name="sf"),
                _PlainModel(id=2, name="plain"),
            ]
        )

    assert mock_bm.call_count == 2
    calls = mock_bm.call_args_list
    # First group is Snowflake-tuned INSERT → render_nulls True
    args0, kwargs0 = calls[0]
    args1, kwargs1 = calls[1]
    if IS_VERSION_20:
        assert kwargs0.get("render_nulls") is True
        assert kwargs1.get("render_nulls") is False
    else:
        assert args0[7] is True
        assert args1[7] is False


def test_bulk_save_objects_manual_flag_gets_render_nulls(sqlite_engine):
    _TaggedManualBulk.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)
    row = _TaggedManualBulk(id=1, label="x")

    with patch.object(
        SnowflakeSession, "_bulk_save_mappings", autospec=True
    ) as mock_bm:
        session.bulk_save_objects([row])

    mock_bm.assert_called_once()
    _args, kwargs = mock_bm.call_args
    if IS_VERSION_20:
        assert kwargs.get("render_nulls") is True
    else:
        assert _args[7] is True


def test_plain_session_sf_model_still_default_render_nulls_false(sqlite_engine):
    """Regression: stock Session must not implicitly enable Snowflake bulk tuning."""
    _SfModel.metadata.create_all(sqlite_engine)
    session = Session(bind=sqlite_engine)
    row = _SfModel(id=1, name="a")

    with patch.object(Session, "_bulk_save_mappings", autospec=True) as mock_bm:
        session.bulk_save_objects([row])

    mock_bm.assert_called_once()
    _args, kwargs = mock_bm.call_args
    if IS_VERSION_20:
        assert kwargs.get("render_nulls") is False
    else:
        assert _args[7] is False


def test_e2e_bulk_save_objects_inserts_optional_columns(sqlite_engine):
    """Mixed optional values persist; uniform explicit keys recommended for batching."""
    _WideBulkModel.metadata.create_all(sqlite_engine)
    session = SnowflakeSession(bind=sqlite_engine)
    session.bulk_save_objects(
        [
            _WideBulkModel(id=1, name="a", tag="t1", note=None),
            _WideBulkModel(id=2, name="b", tag=None, note=None),
            _WideBulkModel(id=3, name="c", tag=None, note="n3"),
        ]
    )
    session.commit()

    with session.bind.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name, tag, note FROM snowflake_orm_wide ORDER BY id")
        ).fetchall()
    assert len(rows) == 3
    assert rows[0] == (1, "a", "t1", None)
    assert rows[1] == (2, "b", None, None)
    assert rows[2][0:2] == (3, "c")


def test_e2e_bulk_insert_uniform_mapping_keys_single_insert(sqlite_engine):
    """Same keys in every mapping (explicit NULLs) → one INSERT with SnowflakeSession."""
    _WideBulkModel.metadata.create_all(sqlite_engine)
    statements: list[str] = []

    def _capture(
        conn,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ):
        if "INSERT" in statement.upper():
            statements.append(statement)

    event.listen(sqlite_engine, "before_cursor_execute", _capture)
    try:
        session = SnowflakeSession(bind=sqlite_engine)
        session.bulk_insert_mappings(
            _WideBulkModel,
            [
                {"id": 1, "name": "a", "tag": None, "note": None},
                {"id": 2, "name": "b", "tag": "x", "note": None},
            ],
        )
        session.commit()
    finally:
        event.remove(sqlite_engine, "before_cursor_execute", _capture)

    assert len(statements) == 1


def test_e2e_bulk_insert_missing_keys_split_batches(sqlite_engine):
    """Differing mapping keys still produce multiple INSERTs (SQLAlchemy grouping)."""
    _WideBulkModel.metadata.create_all(sqlite_engine)
    statements: list[str] = []

    def _capture(
        conn,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ):
        if "INSERT" in statement.upper():
            statements.append(statement)

    event.listen(sqlite_engine, "before_cursor_execute", _capture)
    try:
        session = SnowflakeSession(bind=sqlite_engine)
        session.bulk_insert_mappings(
            _WideBulkModel,
            [
                {"id": 1, "name": "a"},
                {"id": 2, "name": "b", "tag": "x"},
            ],
        )
        session.commit()
    finally:
        event.remove(sqlite_engine, "before_cursor_execute", _capture)

    assert len(statements) == 2


def test_e2e_bulk_save_objects_uniform_optional_keys_single_insert(sqlite_engine):
    """Explicit None on all optional columns → one INSERT with SnowflakeSession."""
    _WideBulkModel.metadata.create_all(sqlite_engine)
    statements: list[str] = []

    def _capture(
        conn,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ):
        if "INSERT" in statement.upper():
            statements.append(statement)

    event.listen(sqlite_engine, "before_cursor_execute", _capture)
    try:
        session = SnowflakeSession(bind=sqlite_engine)
        session.bulk_save_objects(
            [
                _WideBulkModel(id=1, name="a", tag=None, note=None),
                _WideBulkModel(id=2, name="b", tag="x", note=None),
            ]
        )
        session.commit()
    finally:
        event.remove(sqlite_engine, "before_cursor_execute", _capture)

    assert len(statements) == 1


def test_e2e_bulk_save_objects_omitted_optional_splits_batches(sqlite_engine):
    """Omitting an optional attribute on one row still yields multiple INSERTs."""
    _WideBulkModel.metadata.create_all(sqlite_engine)
    statements: list[str] = []

    def _capture(
        conn,
        cursor,
        statement,
        parameters,
        context,
        executemany,
    ):
        if "INSERT" in statement.upper():
            statements.append(statement)

    event.listen(sqlite_engine, "before_cursor_execute", _capture)
    try:
        session = SnowflakeSession(bind=sqlite_engine)
        session.bulk_save_objects(
            [
                _WideBulkModel(id=1, name="a"),
                _WideBulkModel(id=2, name="b", tag="x", note=None),
            ]
        )
        session.commit()
    finally:
        event.remove(sqlite_engine, "before_cursor_execute", _capture)

    assert len(statements) == 2
