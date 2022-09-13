#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy import Integer, String, and_, select
from sqlalchemy.schema import DropColumnComment, DropTableComment
from sqlalchemy.sql import column, quoted_name, table
from sqlalchemy.testing import AssertsCompiledSQL

from snowflake.sqlalchemy.custom_types import ARRAY

table1 = table(
    "table1", column("id", Integer), column("name", String), column("value", Integer)
)

table2 = table(
    "table2",
    column("id", Integer),
    column("name", String),
    column("value", Integer),
    schema="test",
)


class TestSnowflakeCompiler(AssertsCompiledSQL):
    __dialect__ = "snowflake"

    def test_multi_table_delete(self):
        statement = table1.delete().where(table1.c.id == table2.c.id)
        self.assert_compile(
            statement,
            "DELETE FROM table1 USING test.table2 WHERE table1.id = test.table2.id",
            dialect="snowflake",
        )

    def test_multi_table_delete_multiple(self):
        statement = table1.delete().where(
            and_(
                table1.c.id == table2.c.id,
                table1.c.name == table2.c.name,
                table1.c.id >= 42,
            )
        )
        self.assert_compile(
            statement,
            "DELETE FROM table1 USING test.table2 WHERE table1.id = test.table2.id "
            "AND table1.name = test.table2.name "
            "AND table1.id >= %(id_1)s",
        )

    def test_multi_table_update(self):
        statement = (
            table1.update()
            .values(name=table2.c.name)
            .where(table1.c.id == table2.c.name)
        )
        self.assert_compile(
            statement,
            "UPDATE table1 SET name=test.table2.name FROM test.table2 "
            "WHERE table1.id = test.table2.name",
        )

    def test_drop_table_comment(self):
        self.assert_compile(DropTableComment(table1), "COMMENT ON TABLE table1 IS ''")
        self.assert_compile(
            DropTableComment(table2), "COMMENT ON TABLE test.table2 IS ''"
        )

    def test_drop_column_comment(self):
        self.assert_compile(
            DropColumnComment(table1.c.id),
            "ALTER TABLE table1 ALTER COLUMN id UNSET COMMENT",
        )
        self.assert_compile(
            DropColumnComment(table2.c.id),
            "ALTER TABLE test.table2 ALTER COLUMN id UNSET COMMENT",
        )

    def test_array_column(self):
        temp_table = table("temp_table", column("array", ARRAY))
        statement = temp_table.insert().values(array=[123, "abc", True])
        self.assert_compile(
            statement,
            "INSERT INTO temp_table (array) SELECT ARRAY_CONSTRUCT(%(array)s)",
        )

        temp_table = table(
            "temp_table",
            column("id", Integer),
            column("array", ARRAY),
            column("name", String),
        )
        statement = temp_table.insert().values(
            id=1, array=[123, "abc", True], name="test"
        )
        self.assert_compile(
            statement,
            "INSERT INTO temp_table (id, array, name) SELECT %(id)s, ARRAY_CONSTRUCT(%(array)s), %(name)s",
        )


def test_quoted_name_label(engine_testaccount):
    test_cases = [
        # quote name
        {
            "label": quoted_name("alias", True),
            "output": 'SELECT colname AS "alias" \nFROM abc GROUP BY colname',
        },
        # not quote label
        {
            "label": "alias",
            "output": "SELECT colname AS alias \nFROM abc GROUP BY colname",
        },
        # not quote mixed case label
        {
            "label": "Alias",
            "output": 'SELECT colname AS "Alias" \nFROM abc GROUP BY colname',
        },
    ]

    for t in test_cases:
        col = column("colname").label(t["label"])
        sel_from_tbl = select(col).group_by(col).select_from(table("abc"))
        compiled_result = sel_from_tbl.compile()
        assert str(compiled_result) == t["output"]
