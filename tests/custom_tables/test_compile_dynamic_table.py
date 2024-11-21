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
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import GEOMETRY, DynamicTable
from snowflake.sqlalchemy.exc import MultipleErrors
from snowflake.sqlalchemy.sql.custom_schema.options import (
    AsQueryOption,
    IdentifierOption,
    KeywordOption,
    LiteralOption,
    TargetLagOption,
    TimeUnit,
)
from snowflake.sqlalchemy.sql.custom_schema.options.keywords import SnowflakeKeyword


def test_compile_dynamic_table(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_dynamic_table"
    test_geometry = DynamicTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", GEOMETRY),
        target_lag=(10, TimeUnit.SECONDS),
        warehouse="warehouse",
        as_query="SELECT * FROM table",
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


@pytest.mark.parametrize(
    "refresh_mode_keyword",
    [
        SnowflakeKeyword.AUTO,
        SnowflakeKeyword.FULL,
        SnowflakeKeyword.INCREMENTAL,
    ],
)
def test_compile_dynamic_table_with_refresh_mode(
    sql_compiler, snapshot, refresh_mode_keyword
):
    metadata = MetaData()
    table_name = "test_dynamic_table"
    test_geometry = DynamicTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", GEOMETRY),
        target_lag=(10, TimeUnit.SECONDS),
        warehouse="warehouse",
        as_query="SELECT * FROM table",
        refresh_mode=refresh_mode_keyword,
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_dynamic_table_with_options_objects(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_dynamic_table"
    test_geometry = DynamicTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", GEOMETRY),
        target_lag=TargetLagOption(10, TimeUnit.SECONDS),
        warehouse=IdentifierOption("warehouse"),
        as_query=AsQueryOption("SELECT * FROM table"),
        refresh_mode=KeywordOption(SnowflakeKeyword.AUTO),
    )

    value = CreateTable(test_geometry)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_dynamic_table_with_one_wrong_option_types(snapshot):
    metadata = MetaData()
    table_name = "test_dynamic_table"
    with pytest.raises(ArgumentError) as argument_error:
        DynamicTable(
            table_name,
            metadata,
            Column("id", Integer),
            Column("geom", GEOMETRY),
            target_lag=TargetLagOption(10, TimeUnit.SECONDS),
            warehouse=LiteralOption("warehouse"),
            as_query=AsQueryOption("SELECT * FROM table"),
            refresh_mode=KeywordOption(SnowflakeKeyword.AUTO),
        )

    assert str(argument_error.value) == snapshot


def test_compile_dynamic_table_with_multiple_wrong_option_types(snapshot):
    metadata = MetaData()
    table_name = "test_dynamic_table"
    with pytest.raises(MultipleErrors) as argument_error:
        DynamicTable(
            table_name,
            metadata,
            Column("id", Integer),
            Column("geom", GEOMETRY),
            target_lag=IdentifierOption(SnowflakeKeyword.AUTO),
            warehouse=KeywordOption(SnowflakeKeyword.AUTO),
            as_query=KeywordOption(SnowflakeKeyword.AUTO),
            refresh_mode=IdentifierOption(SnowflakeKeyword.AUTO),
        )

    assert str(argument_error.value) == snapshot


def test_compile_dynamic_table_without_required_args(sql_compiler):
    with pytest.raises(
        exc.ArgumentError,
        match="DynamicTable requires the following parameters: warehouse, "
        "as_query, target_lag.",
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
        match="Primary key and foreign keys are not supported in DynamicTable.",
    ):
        DynamicTable(
            "test_dynamic_table",
            MetaData(),
            Column("id", Integer, primary_key=True),
            Column("geom", GEOMETRY),
            target_lag=(10, TimeUnit.SECONDS),
            warehouse="warehouse",
            as_query="SELECT * FROM table",
        )


def test_compile_dynamic_table_with_foreign_key(sql_compiler):
    with pytest.raises(
        exc.ArgumentError,
        match="Primary key and foreign keys are not supported in DynamicTable.",
    ):
        DynamicTable(
            "test_dynamic_table",
            MetaData(),
            Column("id", Integer),
            Column("geom", GEOMETRY),
            ForeignKeyConstraint(["id"], ["table.id"]),
            target_lag=(10, TimeUnit.SECONDS),
            warehouse="warehouse",
            as_query="SELECT * FROM table",
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
        target_lag=(10, TimeUnit.SECONDS),
        warehouse="warehouse",
        as_query="SELECT * FROM table",
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

        __table_args__ = {
            "schema": "SCHEMA_DB",
            "target_lag": (10, TimeUnit.SECONDS),
            "warehouse": "warehouse",
            "as_query": "SELECT * FROM table",
        }

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
        target_lag=(10, TimeUnit.SECONDS),
        warehouse="warehouse",
        as_query=select(test_table_1).where(test_table_1.c.id == 23),
    )

    value = CreateTable(dynamic_test_table)

    actual = sql_compiler(value)

    assert actual == snapshot
