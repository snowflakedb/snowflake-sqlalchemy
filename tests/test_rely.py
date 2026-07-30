#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Table,
    UniqueConstraint,
)
from sqlalchemy.schema import CreateTable


def test_primary_key_rely(sql_compiler):
    """snowflake_rely=True renders RELY on a PRIMARY KEY constraint (SNOW-1023317)."""
    metadata = MetaData()
    table = Table(
        "t_pk",
        metadata,
        Column("id", Integer),
        PrimaryKeyConstraint("id", snowflake_rely=True),
    )
    ddl = sql_compiler(CreateTable(table))
    assert "PRIMARY KEY (id) RELY" in ddl


def test_foreign_key_rely(sql_compiler):
    """snowflake_rely=True renders RELY on a FOREIGN KEY constraint (SNOW-1023317)."""
    metadata = MetaData()
    Table("parent", metadata, Column("id", Integer, primary_key=True))
    child = Table(
        "child",
        metadata,
        Column("pid", Integer),
        ForeignKeyConstraint(["pid"], ["parent.id"], snowflake_rely=True),
    )
    ddl = sql_compiler(CreateTable(child))
    assert "REFERENCES parent (id) RELY" in ddl


def test_unique_constraint_rely(sql_compiler):
    """snowflake_rely=True renders RELY on a UNIQUE constraint (SNOW-1023317)."""
    metadata = MetaData()
    table = Table(
        "t_uq",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("email", String),
        UniqueConstraint("email", snowflake_rely=True),
    )
    ddl = sql_compiler(CreateTable(table))
    assert "UNIQUE (email) RELY" in ddl


def test_no_rely_by_default(sql_compiler):
    """Constraints without snowflake_rely are unchanged (no RELY emitted)."""
    metadata = MetaData()
    table = Table(
        "t_default",
        metadata,
        Column("id", Integer),
        PrimaryKeyConstraint("id"),
    )
    ddl = sql_compiler(CreateTable(table))
    assert "RELY" not in ddl
    assert "PRIMARY KEY (id)" in ddl


def test_rely_false_does_not_render(sql_compiler):
    """snowflake_rely=False behaves like the default (no RELY)."""
    metadata = MetaData()
    table = Table(
        "t_false",
        metadata,
        Column("id", Integer),
        PrimaryKeyConstraint("id", snowflake_rely=False),
    )
    ddl = sql_compiler(CreateTable(table))
    assert "RELY" not in ddl
