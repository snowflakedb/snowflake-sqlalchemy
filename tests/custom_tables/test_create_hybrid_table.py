#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
import sqlalchemy.exc
from sqlalchemy import Column, Index, Integer, MetaData, String, select
from sqlalchemy.orm import Session, declarative_base

from snowflake.sqlalchemy import HybridTable


@pytest.mark.aws
def test_create_hybrid_table(engine_testaccount, db_parameters, snapshot):
    metadata = MetaData()
    table_name = "test_create_hybrid_table"

    dynamic_test_table_1 = HybridTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )

    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        ins = dynamic_test_table_1.insert().values(id=1, name="test")
        conn.execute(ins)
        conn.commit()

    try:
        with engine_testaccount.connect() as conn:
            s = select(dynamic_test_table_1)
            results_hybrid_table = conn.execute(s).fetchall()
            assert str(results_hybrid_table) == snapshot
    finally:
        metadata.drop_all(engine_testaccount)


@pytest.mark.aws
def test_create_hybrid_table_with_multiple_index(
    engine_testaccount, db_parameters, snapshot, sql_compiler
):
    metadata = MetaData()
    table_name = "test_hybrid_table_with_multiple_index"

    hybrid_test_table_1 = HybridTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String, index=True),
        Column("name2", String),
        Column("name3", String),
    )

    metadata.create_all(engine_testaccount)

    index = Index("idx_col34", hybrid_test_table_1.c.name2, hybrid_test_table_1.c.name3)

    with pytest.raises(sqlalchemy.exc.ProgrammingError) as exc_info:
        index.create(engine_testaccount)
    try:
        assert exc_info.value == snapshot
    finally:
        metadata.drop_all(engine_testaccount)


@pytest.mark.aws
def test_create_hybrid_table_with_orm(sql_compiler, engine_testaccount):
    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestHybridTableOrm(Base):
        __tablename__ = "test_hybrid_table_orm"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return HybridTable(name, metadata, *arg, **kw)

        id = Column(Integer, primary_key=True)
        name = Column(String)

        def __repr__(self):
            return f"({self.id!r}, {self.name!r})"

    Base.metadata.create_all(engine_testaccount)

    try:
        instance = TestHybridTableOrm(id=0, name="name_example")
        session.add(instance)
        session.commit()
        data = session.query(TestHybridTableOrm).all()
        assert str(data) == "[(0, 'name_example')]"
    finally:
        Base.metadata.drop_all(engine_testaccount)
