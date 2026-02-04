#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import re

import pytest
from sqlalchemy import Column, Integer, MetaData, String, inspect, select
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.schema import CreateTable, Table
from sqlalchemy.sql.sqltypes import Float as SAFloat
from sqlalchemy.sql.sqltypes import Integer as SAInteger
from sqlalchemy.sql.sqltypes import Text as SAText

from snowflake.sqlalchemy import snowdialect
from snowflake.sqlalchemy.custom_types import VECTOR

from .util import random_string


def _normalize_ddl(ddl: str) -> str:
    """Normalize DDL string by removing extra whitespace and newlines."""
    return re.sub(r"\s+", " ", ddl).strip()


class TestVectorTypeUnit:
    """Unit tests for VECTOR type normalization and error paths."""

    def test_vector_visit_name(self):
        assert VECTOR("FLOAT", 3).__visit_name__ == "VECTOR"

    def test_vector_repr(self):
        assert repr(VECTOR("FLOAT", 3)) == "VECTOR(FLOAT, 3)"
        assert repr(VECTOR(" int ", 256)) == "VECTOR(INT, 256)"

    def test_vector_accepts_sqlalchemy_types(self):
        assert repr(VECTOR(SAInteger(), 8)) == "VECTOR(INT, 8)"
        assert repr(VECTOR(SAFloat(), 2)) == "VECTOR(FLOAT, 2)"

    def test_vector_allows_large_dimension(self):
        # NOTE: Snowflake enforces maximum dimension (e.g. 4096) server-side.
        # The type does not validate that limit.
        assert VECTOR("FLOAT", 5000).dimension == 5000

    def test_vector_rejects_invalid_dimension_bad_paths(self):
        with pytest.raises(ValueError):
            VECTOR("FLOAT", 0)
        with pytest.raises(ValueError):
            VECTOR("FLOAT", -10)
        with pytest.raises(TypeError):
            VECTOR("FLOAT", "10")  # type: ignore[arg-type]

    def test_vector_rejects_invalid_element_type_bad_paths(self):
        with pytest.raises(ValueError):
            VECTOR("STRING", 10)
        with pytest.raises(TypeError):
            VECTOR(SAText(), 10)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            VECTOR(123, 10)  # type: ignore[arg-type]

    def test_vector_rejects_invalid_sqlalchemy_type_mapping_bad_path(self):
        with pytest.raises(ValueError):
            VECTOR._map_sqlalchemy_type()  # type: ignore[arg-type]

    def test_compile_create_table_with_vector(self):
        metadata = MetaData()
        test_table = Table(
            "vector_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("float_vec_str", VECTOR("FLOAT", 3)),
            Column("int_vec_str", VECTOR("INT", 4)),
            Column("float_vec", VECTOR(SAFloat(), 5)),
            Column("int_vec", VECTOR(SAInteger(), 6)),
            Column("name", String),
        )

        ddl = str(CreateTable(test_table).compile(dialect=snowdialect.dialect()))
        expected = (
            "CREATE TABLE vector_table ( "
            "id INTEGER NOT NULL AUTOINCREMENT, "
            "float_vec_str VECTOR(FLOAT, 3), "
            "int_vec_str VECTOR(INT, 4), "
            "float_vec VECTOR(FLOAT, 5), "
            "int_vec VECTOR(INT, 6), "
            "name VARCHAR, "
            "PRIMARY KEY (id) )"
        )
        assert _normalize_ddl(ddl) == expected


class TestVectorIntegration:
    """Integration tests for VECTOR against a real Snowflake account."""

    def test_vector_reflection_round_trip(self, engine_testaccount):
        table_name = "test_vector_" + random_string(8)
        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(
                        f"CREATE TABLE {table_name} (id INTEGER, float_vec VECTOR(FLOAT, 3))"
                    )

            insp = inspect(engine_testaccount)
            cols = insp.get_columns(table_name)
            float_vec_col = next(c for c in cols if c["name"].lower() == "float_vec")

            assert isinstance(float_vec_col["type"], VECTOR)
            assert float_vec_col["type"].element_type == "FLOAT"
            assert float_vec_col["type"].dimension == 3
        finally:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_vector_dimension_limit_enforced_by_snowflake(self, engine_testaccount):
        # Standard accounts enforce a max vector dimension of 4096.
        table_name = "test_vector_dim_" + random_string(8)
        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    with pytest.raises(ProgrammingError) as e:
                        conn.exec_driver_sql(
                            f"CREATE TABLE {table_name} (v VECTOR(FLOAT, 5000))"
                        )
                    assert "invalid vector dimension '5,000'" in str(e).lower()
        finally:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_vector_core_insert_select_happy_path(self, engine_testaccount):
        table_name = "test_vector_core_" + random_string(8)
        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(
                        f"CREATE TABLE {table_name} (id INTEGER, float_vec VECTOR(FLOAT, 3))"
                    )
                    conn.exec_driver_sql(
                        f"INSERT INTO {table_name}(id, float_vec) "
                        f"SELECT 1, [1.0, 2.0, 3.0]::VECTOR(FLOAT, 3)"
                    )
                    row = conn.exec_driver_sql(
                        f"SELECT float_vec FROM {table_name} WHERE id=1"
                    ).fetchone()
                    assert row is not None
                    assert row[0] == [1.0, 2.0, 3.0]
        finally:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_vector_core_rejects_wrong_dimension_bad_path(self, engine_testaccount):
        table_name = "test_vector_bad_dim_" + random_string(8)
        try:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(
                        f"CREATE TABLE {table_name} (id INTEGER, float_vec VECTOR(FLOAT, 3))"
                    )
                    with pytest.raises(ProgrammingError) as e:
                        conn.exec_driver_sql(
                            f"INSERT INTO {table_name}(id, float_vec) "
                            f"SELECT 1, [1.0, 2.0]::VECTOR(FLOAT, 2)"
                        )
                    assert (
                        "expression type does not match column data" in str(e).lower()
                    )
        finally:
            with engine_testaccount.connect() as conn:
                with conn.begin():
                    conn.exec_driver_sql(f"DROP TABLE IF EXISTS {table_name}")

    def test_vector_orm_create_and_query(self, engine_testaccount):
        Base = declarative_base()
        table_name = "test_vector_orm_" + random_string(8)

        class TestTable(Base):
            __tablename__ = table_name

            id = Column(Integer, primary_key=True)
            float_vec_str = Column(VECTOR("FLOAT", 3))
            int_vec_str = Column(VECTOR("INT", 3))
            float_vec = Column(VECTOR(SAFloat(), 3))
            int_vec = Column(VECTOR(SAInteger(), 3))

        Base.metadata.create_all(engine_testaccount)
        try:
            with engine_testaccount.begin() as conn:
                conn.exec_driver_sql(
                    f"INSERT INTO {table_name}(id, float_vec_str, int_vec_str, float_vec, int_vec) SELECT 1, [1.0, 2.0, 3.0]::VECTOR(FLOAT, 3), [1, 2, 3]::VECTOR(INT, 3), [1.0, 2.0, 3.0]::VECTOR(FLOAT, 3), [1, 2, 3]::VECTOR(INT, 3)"
                )

            with Session(bind=engine_testaccount) as session:
                row = session.execute(
                    select(TestTable).where(TestTable.id == 1)
                ).scalar_one()
                assert row.id == 1
                assert row.float_vec_str == [1.0, 2.0, 3.0]
                assert row.float_vec == [1.0, 2.0, 3.0]
                assert row.int_vec_str == [1, 2, 3]
                assert row.int_vec == [1, 2, 3]
        finally:
            Base.metadata.drop_all(engine_testaccount)
