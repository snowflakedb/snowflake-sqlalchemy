#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Live round-trip tests for enable_structured_type_json (write + read).

These require a Snowflake account with an active warehouse (as configured for
the dialect test suite); they exercise the full path: Python dict/list ->
PARSE_JSON via INSERT ... SELECT -> stored VARIANT/OBJECT/ARRAY -> json.loads
on read.
"""

import uuid

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Table,
    create_engine,
    insert,
    select,
    update,
)
from sqlalchemy.orm import Session, declarative_base

from snowflake.sqlalchemy import ARRAY, OBJECT, VARIANT

from .conftest import url_factory


@pytest.fixture()
def engine_json(db_parameters):
    engine = create_engine(url_factory(enable_structured_type_json=True))
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture()
def semi_structured_table(engine_json):
    meta = MetaData()
    table = Table(
        f"json_rt_{uuid.uuid4().hex}",
        meta,
        Column("id", Integer, primary_key=True),
        Column("v", VARIANT),
        Column("o", OBJECT),
        Column("a", ARRAY),
    )
    table.create(engine_json)
    try:
        yield table
    finally:
        table.drop(engine_json)


def test_single_row_roundtrip(engine_json, semi_structured_table):
    t = semi_structured_table
    with engine_json.begin() as conn:
        conn.execute(
            insert(t).values(id=1, v={"x": 1, "y": [2, 3]}, o={"k": "val"}, a=[1, 2, 3])
        )
    with engine_json.connect() as conn:
        row = conn.execute(select(t).where(t.c.id == 1)).one()
    assert row.v == {"x": 1, "y": [2, 3]}
    assert row.o == {"k": "val"}
    assert row.a == [1, 2, 3]


@pytest.mark.xfail(
    reason=(
        "executemany with 2+ parameter sets is unsupported while "
        "enable_structured_type_json is on: the connector tries to fold the rows "
        "into a single VALUES clause, which the INSERT ... SELECT PARSE_JSON(...) "
        "rewrite does not have, raising 252001 'Failed to rewrite multi-row "
        "insert'. Use insert().values([...]) (UNION ALL) for multi-row instead."
    ),
    strict=True,
    raises=Exception,
)
def test_executemany_roundtrip(engine_json, semi_structured_table):
    t = semi_structured_table
    with engine_json.begin() as conn:
        conn.execute(
            insert(t),
            [{"id": 2, "v": {"n": 2}}, {"id": 3, "v": {"n": 3}}],
        )
    with engine_json.connect() as conn:
        rows = conn.execute(
            select(t.c.id, t.c.v).where(t.c.id.in_([2, 3])).order_by(t.c.id)
        ).all()
    assert [r.v for r in rows] == [{"n": 2}, {"n": 3}]


def test_multivalues_roundtrip(engine_json, semi_structured_table):
    t = semi_structured_table
    with engine_json.begin() as conn:
        conn.execute(
            insert(t).values([{"id": 4, "v": {"m": 4}}, {"id": 5, "v": {"m": 5}}])
        )
    with engine_json.connect() as conn:
        rows = conn.execute(
            select(t.c.id, t.c.v).where(t.c.id.in_([4, 5])).order_by(t.c.id)
        ).all()
    assert [r.v for r in rows] == [{"m": 4}, {"m": 5}]


def test_update_roundtrip(engine_json, semi_structured_table):
    t = semi_structured_table
    with engine_json.begin() as conn:
        conn.execute(insert(t).values(id=6, v={"before": True}))
    with engine_json.begin() as conn:
        conn.execute(update(t).where(t.c.id == 6).values(v={"after": [1, 2]}))
    with engine_json.connect() as conn:
        row = conn.execute(select(t.c.v).where(t.c.id == 6)).one()
    assert row.v == {"after": [1, 2]}


@pytest.fixture()
def orm_annotation_model(engine_json):
    """Declarative model mirroring the SNOW-177555 customer schema (an ``OBJECT``
    column populated with a plain Python ``dict`` via the ORM)."""
    base = declarative_base()

    class Annotation(base):
        __tablename__ = f"json_orm_{uuid.uuid4().hex}"

        id = Column(Integer, primary_key=True)
        meta_data = Column(OBJECT)
        tags = Column(ARRAY)
        payload = Column(VARIANT)

    base.metadata.create_all(engine_json)
    try:
        yield Annotation
    finally:
        base.metadata.drop_all(engine_json)


def test_orm_session_add_roundtrip(engine_json, orm_annotation_model):
    """SNOW-177555: the customer's exact ORM usage — ``session.add`` of an object
    whose untyped ``OBJECT`` column holds a Python ``dict`` — must persist (via
    PARSE_JSON) and read back as the original dict, instead of failing with
    "Binding data in type (dict) is not supported"."""
    Annotation = orm_annotation_model
    with Session(engine_json) as session:
        session.add(
            Annotation(
                id=1,
                meta_data={"x_val": "2020-07-05", "load_type": 1},
                tags=[1, 2, 3],
                payload={"nested": {"a": 1}},
            )
        )
        session.commit()

    with Session(engine_json) as session:
        obj = session.get(Annotation, 1)
        assert obj.meta_data == {"x_val": "2020-07-05", "load_type": 1}
        assert obj.tags == [1, 2, 3]
        assert obj.payload == {"nested": {"a": 1}}
