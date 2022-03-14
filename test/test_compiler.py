#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Integer, JSON, String, and_, select
from sqlalchemy.sql import column, quoted_name, table
from sqlalchemy.testing import AssertsCompiledSQL

table1 = table(
    'table1',
    column('id', Integer),
    column('name', String),
    column('value', Integer),
    column('metadata', JSON)
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
        statement = table1.update() \
            .values(name=table2.c.name) \
            .where(table1.c.id == table2.c.name)
        self.assert_compile(
            statement,
            "UPDATE table1 SET name=table2.name FROM table2 "
            "WHERE table1.id = table2.name"
        )


    def test_json_getitem(self):
        statement = table1.select() \
            .with_only_columns([table1.c.metadata['size'].label('size')])
        self.assert_compile(
            statement,
            "SELECT table1.metadata['size'] AS size FROM table1"
        )


def test_quoted_name_label(engine_testaccount):
    test_cases = [
        # quote name
        {"label": quoted_name('alias', True),
         "output": 'SELECT colname AS "alias" \nFROM abc GROUP BY colname'},
        # not quote label
        {"label": 'alias',
         "output": 'SELECT colname AS alias \nFROM abc GROUP BY colname'},
        # not quote mixed case label
        {"label": 'Alias',
         "output": 'SELECT colname AS "Alias" \nFROM abc GROUP BY colname'},
    ]

    for t in test_cases:
        col = column('colname').label(t["label"])
        sel_from_tbl = select([col]).group_by(col).select_from(table('abc'))
        compiled_result = sel_from_tbl.compile()
        assert str(compiled_result) == t["output"]
