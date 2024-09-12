#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    exc,
    select,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import GEOMETRY, DynamicTable
from snowflake.sqlalchemy.sql.custom_schema.options.as_query import AsQuery
from snowflake.sqlalchemy.sql.custom_schema.options.target_lag import (
    TargetLag,
    TimeUnit,
)
from snowflake.sqlalchemy.sql.custom_schema.options.warehouse import Warehouse


def test_compile_dynamic_table(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_dynamic_table"
    test_geometry = DynamicTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", GEOMETRY),
        TargetLag(10, TimeUnit.SECONDS),
        Warehouse("warehouse"),
        AsQuery("SELECT * FROM table"),
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_dynamic_table_without_required_args(sql_compiler):
    with pytest.raises(
        exc.ArgumentError,
        match="DYNAMIC TABLE must have the following arguments: TargetLag, "
        "Warehouse, AsQuery",
    ):
        DynamicTable(
            "test_dynamic_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", GEOMETRY),
        )


def test_compile_dynamic_table_with_primary_key(sql_compiler):
    with pytest.raises(
        exc.ArgumentError,
        match="Primary key and foreign keys are not supported in DYNAMIC TABLE.",
    ):
        DynamicTable(
            "test_dynamic_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", GEOMETRY),
            TargetLag(10, TimeUnit.SECONDS),
            Warehouse("warehouse"),
            AsQuery("SELECT * FROM table"),
        )


def test_compile_dynamic_table_with_foreign_key(sql_compiler):
    with pytest.raises(
        exc.ArgumentError,
        match="Primary key and foreign keys are not supported in DYNAMIC TABLE.",
    ):
        DynamicTable(
            "test_dynamic_table",
            MetaData(),
            Column("id", Integer),
            Column("geom", GEOMETRY),
            TargetLag(10, TimeUnit.SECONDS),
            Warehouse("warehouse"),
            AsQuery("SELECT * FROM table"),
            ForeignKeyConstraint(["id"], ["table.id"]),
        )


def test_compile_dynamic_table_orm(sql_compiler, snapshot):
    Base = declarative_base()
    metadata = MetaData()
    table_name = "test_dynamic_table_orm"
    test_dynamic_table_orm = DynamicTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("name", String),
        TargetLag(10, TimeUnit.SECONDS),
        Warehouse("warehouse"),
        AsQuery("SELECT * FROM table"),
    )

    class TestDynamicTableOrm(Base):
        __table__ = test_dynamic_table_orm
        __mapper_args__ = {
            "primary_key": [test_dynamic_table_orm.c.id, test_dynamic_table_orm.c.name]
        }

        def __repr__(self):
            return f"<TestDynamicTableOrm({self.name!r}, {self.fullname!r})>"

    value = CreateTable(TestDynamicTableOrm.__table__)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_dynamic_table_orm_with_str_keys(sql_compiler, snapshot):
    Base = declarative_base()

    class TestDynamicTableOrm(Base):
        __tablename__ = "test_dynamic_table_orm_2"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return DynamicTable(name, metadata, *arg, **kw)

        __table_args__ = (
            TargetLag(10, TimeUnit.SECONDS),
            Warehouse("warehouse"),
            AsQuery("SELECT * FROM table"),
        )

        id = Column(Integer)
        name = Column(String)

        __mapper_args__ = {"primary_key": [id, name]}

        def __repr__(self):
            return f"<TestDynamicTableOrm({self.name!r}, {self.fullname!r})>"

    value = CreateTable(TestDynamicTableOrm.__table__)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_dynamic_table_with_selectable(sql_compiler, snapshot):
    Base = declarative_base()

    test_table_1 = Table(
        "test_table_1",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String),
    )

    dynamic_test_table = DynamicTable(
        "dynamic_test_table_1",
        Base.metadata,
        TargetLag(10, TimeUnit.SECONDS),
        Warehouse("warehouse"),
        AsQuery(select(test_table_1).where(test_table_1.c.id == 23)),
    )

    value = CreateTable(dynamic_test_table)

    actual = sql_compiler(value)

    assert actual == snapshot
