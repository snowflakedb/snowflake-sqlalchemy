#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import GEOMETRY, HybridTable


@pytest.mark.aws
def test_compile_hybrid_table(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_hybrid_table"
    test_geometry = HybridTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        Column("geom", GEOMETRY),
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


@pytest.mark.aws
def test_compile_hybrid_table_orm(sql_compiler, snapshot):
    Base = declarative_base()

    class TestHybridTableOrm(Base):
        __tablename__ = "test_hybrid_table_orm"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return HybridTable(name, metadata, *arg, **kw)

        id = Column(Integer, primary_key=True)
        name = Column(String)

        def __repr__(self):
            return f"<TestHybridTableOrm({self.name!r}, {self.fullname!r})>"

    value = CreateTable(TestHybridTableOrm.__table__)

    actual = sql_compiler(value)

    assert actual == snapshot
