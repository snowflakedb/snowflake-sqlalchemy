#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from sqlalchemy import Column, Integer, MetaData, String, select, text
from sqlalchemy.orm import Session, declarative_base

from snowflake.sqlalchemy import SnowflakeTable


def test_create_snowflake_table_with_cluster_by(
    engine_testaccount, db_parameters, snapshot
):
    metadata = MetaData()
    table_name = "test_create_snowflake_table"

    test_table_1 = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
        cluster_by=["id", text("id > 5")],
    )

    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        ins = test_table_1.insert().values(id=1, name="test")
        conn.execute(ins)
        conn.commit()

    try:
        with engine_testaccount.connect() as conn:
            s = select(test_table_1)
            results_hybrid_table = conn.execute(s).fetchall()
            assert str(results_hybrid_table) == snapshot
    finally:
        metadata.drop_all(engine_testaccount)


def test_create_snowflake_table_with_orm(sql_compiler, engine_testaccount):
    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestHybridTableOrm(Base):
        __tablename__ = "test_snowflake_table_orm"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return SnowflakeTable(name, metadata, *arg, **kw)

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
