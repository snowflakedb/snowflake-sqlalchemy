#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Live integration test: with enable_structured_type_json on, reading each
semi-structured type maps Snowflake's JSON back to native Python (PR #7 review).

Requires a Snowflake account with an active warehouse (as configured for the
dialect test suite). Data is seeded with explicit PARSE_JSON (the write-side
auto-wrapping lives in a later PR); this test validates the read/result-processor
mapping per type.
"""

import uuid

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, select

from snowflake.sqlalchemy import ARRAY, OBJECT, VARIANT

from .conftest import url_factory


@pytest.fixture()
def read_table(db_parameters):
    engine = create_engine(url_factory(enable_structured_type_json=True))
    meta = MetaData()
    table = Table(
        f"json_read_{uuid.uuid4().hex}",
        meta,
        Column("id", Integer, primary_key=True),
        Column("v", VARIANT),
        Column("o", OBJECT),
        Column("a", ARRAY),
    )
    table.create(engine)
    # Seed with explicit PARSE_JSON (INSERT ... SELECT, since functions are
    # rejected in a VALUES clause) so the read path has real semi-structured data.
    with engine.begin() as conn:
        conn.exec_driver_sql(
            f"INSERT INTO {table.name} (id, v, o, a) "
            'SELECT 1, PARSE_JSON(\'{"x": 1, "y": [2, 3]}\'), '
            "PARSE_JSON('{\"k\": \"val\"}'), PARSE_JSON('[1, 2, 3]')"
        )
    try:
        yield engine, table
    finally:
        table.drop(engine)
        engine.dispose()


def test_each_type_maps_to_python_when_enabled(read_table):
    engine, t = read_table
    with engine.connect() as conn:
        row = conn.execute(select(t.c.v, t.c.o, t.c.a).where(t.c.id == 1)).one()
    assert row.v == {"x": 1, "y": [2, 3]}  # VARIANT -> dict
    assert row.o == {"k": "val"}  # OBJECT  -> dict
    assert row.a == [1, 2, 3]  # ARRAY   -> list


def test_types_stay_raw_json_when_flag_off(read_table):
    _, t = read_table
    # A separate engine with the flag OFF must return raw JSON text (no BCR).
    engine_off = create_engine(url_factory())
    try:
        with engine_off.connect() as conn:
            row = conn.execute(select(t.c.v, t.c.o, t.c.a).where(t.c.id == 1)).one()
        assert all(isinstance(val, str) for val in (row.v, row.o, row.a))
    finally:
        engine_off.dispose()
