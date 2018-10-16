#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Integer, String, and_
from sqlalchemy.sql import column, table
from sqlalchemy.testing import AssertsCompiledSQL


table1 = table(
    'table1',
    column('id', Integer),
    column('name', String),
    column('value', Integer)
)

table2 = table(
    'table2',
    column('id', Integer),
    column('name', String),
    column('value', Integer)
)


class TestSnowflakeCompiler(AssertsCompiledSQL):
    __dialect__ = "snowflake"

    def test_multi_table_delete(self):
        statement = table1.delete().where(table1.c.id == table2.c.id)
        self.assert_compile(
            statement,
            "DELETE FROM table1 USING table2 WHERE table1.id = table2.id",
            dialect='snowflake'
        )

    def test_multi_table_delete_multiple(self):
        statement = table1.delete().where(and_(
            table1.c.id == table2.c.id,
            table1.c.name == table2.c.name,
            table1.c.id >= 42
        ))
        self.assert_compile(
            statement,
            "DELETE FROM table1 USING table2 WHERE table1.id = table2.id "
            "AND table1.name = table2.name "
            "AND table1.id >= %(id_1)s",
        )

    def test_multi_table_update(self):
        statement = table1.update()\
                    .values(name=table2.c.name)\
                    .where(table1.c.id == table2.c.name)
        self.assert_compile(
            statement,
            "UPDATE table1 SET name=table2.name FROM table2 "
            "WHERE table1.id = table2.name"
        )
