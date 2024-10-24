#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy.exc import ArgumentError
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import IcebergTable
from snowflake.sqlalchemy.sql.custom_schema.options import (
    IdentifierOption,
    LiteralOption,
)


def test_compile_iceberg_table(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_iceberg_table"
    test_table = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", String),
        external_volume="my_external_volume",
        base_location="my_iceberg_table",
    )

    value = CreateTable(test_table)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_iceberg_table_with_options_objects(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_iceberg_table_with_options"
    test_table = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer),
        Column("geom", String),
        external_volume=LiteralOption("my_external_volume"),
        base_location=LiteralOption("my_iceberg_table"),
    )

    value = CreateTable(test_table)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_iceberg_table_with_one_wrong_option_types(snapshot):
    metadata = MetaData()
    table_name = "test_wrong_iceberg_table"
    with pytest.raises(ArgumentError) as argument_error:
        IcebergTable(
            table_name,
            metadata,
            Column("id", Integer),
            Column("geom", String),
            external_volume=IdentifierOption("my_external_volume"),
            base_location=LiteralOption("my_iceberg_table"),
        )

    assert str(argument_error.value) == snapshot


def test_compile_icberg_table_with_primary_key(sql_compiler, snapshot):
    metadata = MetaData()
    table_name = "test_iceberg_table_with_options"
    test_table = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        external_volume=LiteralOption("my_external_volume"),
        base_location=LiteralOption("my_iceberg_table"),
    )

    value = CreateTable(test_table)

    actual = sql_compiler(value)

    assert actual == snapshot


def test_compile_dynamic_table_orm_with_as_query(sql_compiler, snapshot):
    Base = declarative_base()

    class TestDynamicTableOrm(Base):
        __tablename__ = "test_iceberg_table_orm_2"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return IcebergTable(name, metadata, *arg, **kw)

        __table_args__ = {
            "external_volume": "my_external_volume",
            "base_location": "my_iceberg_table",
            "as_query": "SELECT * FROM table",
        }

        id = Column(Integer, primary_key=True)
        name = Column(String)

        def __repr__(self):
            return f"<TestDynamicTableOrm({self.name!r}, {self.fullname!r})>"

    value = CreateTable(TestDynamicTableOrm.__table__)

    actual = sql_compiler(value)

    assert actual == snapshot
