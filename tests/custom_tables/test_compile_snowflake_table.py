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
    select,
    text,
)
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import SnowflakeTable
from snowflake.sqlalchemy.sql.custom_schema.options import (
    AsQueryOption,
    ClusterByOption,
)


def test_compile_snowflake_table(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_table_1"
    test_geometry = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", String),
        cluster_by=["id", text("id > 100")],
        as_query="SELECT * FROM table",
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_snowflake_table_with_explicit_options(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_table_2"
    test_geometry = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", String),
        cluster_by=ClusterByOption("id", text("id > 100")),
        as_query=AsQueryOption("SELECT * FROM table"),
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_snowflake_table_with_wrong_option_types(snapshot):
    metadata = MetaData()
    table_name = "test_snowflake_table"
    with pytest.raises(ArgumentError) as argument_error:
        SnowflakeTable(
            table_name,
            metadata,
            Column("id", Integer),
            Column("geom", String),
            as_query=ClusterByOption("id", text("id > 100")),
            cluster_by=AsQueryOption("SELECT * FROM table"),
        )

    assert str(argument_error.value) == snapshot


def test_compile_snowflake_table_with_primary_key(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_table_2"
    test_geometry = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        cluster_by=ClusterByOption("id", text("id > 100")),
        as_query=AsQueryOption("SELECT * FROM table"),
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_snowflake_table_with_foreign_key(sql_compiler, snapshot):
    metadata = MetaData()

    SnowflakeTable(
        "table",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        ForeignKeyConstraint(["id"], ["table.id"]),
        cluster_by=ClusterByOption("id", text("id > 100")),
        as_query=AsQueryOption("SELECT * FROM table"),
    )

    table_name = "test_table_2"
    test_geometry = SnowflakeTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        ForeignKeyConstraint(["id"], ["table.id"]),
        cluster_by=ClusterByOption("id", text("id > 100")),
        as_query=AsQueryOption("SELECT * FROM table"),
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_snowflake_table_orm_with_str_keys(sql_compiler, snapshot):
    Base = declarative_base()

    class TestSnowflakeTableOrm(Base):
        __tablename__ = "test_snowflake_table_orm_2"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return SnowflakeTable(name, metadata, *arg, **kw)

        __table_args__ = {
            "schema": "SCHEMA_DB",
            "cluster_by": ["id", text("id > 100")],
            "as_query": "SELECT * FROM table",
        }

        id = Column(Integer, primary_key=True)
        name = Column(String)

        def __repr__(self):
            return f"<TestSnowflakeTableOrm({self.name!r}, {self.fullname!r})>"

    value = CreateTable(TestSnowflakeTableOrm.__table__)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_snowflake_table_with_selectable(sql_compiler, snapshot):
    Base = declarative_base()

    test_table_1 = SnowflakeTable(
        "test_table_1",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        ForeignKeyConstraint(["id"], ["table.id"]),
        cluster_by=ClusterByOption("id", text("id > 100")),
    )

    test_table_2 = SnowflakeTable(
        "snowflake_test_table_1",
        Base.metadata,
        as_query=select(test_table_1).where(test_table_1.c.id == 23),
    )

    value = CreateTable(test_table_2)

    actual = sql_compiler(value)

    assert actual == snapshot
