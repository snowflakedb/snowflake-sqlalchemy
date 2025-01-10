#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest
from sqlalchemy import Integer, String, and_, func, insert, select
from sqlalchemy.schema import DropColumnComment, DropTableComment
from sqlalchemy.sql import column, quoted_name, table
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from snowflake.sqlalchemy import snowdialect
from src.snowflake.sqlalchemy.snowdialect import SnowflakeDialect

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

    def test_now_func(self):
        statement = select(func.now())
        self.assert_compile(
            statement,
            "SELECT CURRENT_TIMESTAMP AS now_1",
            dialect="snowflake",
        )

    def test_underscore_as_valid_identifier(self):
        _table = table(
            "table_1745924",
            column("ca", Integer),
            column("cb", String),
            column("_", String),
        )

        stmt = insert(_table).values(ca=1, cb="test", _="test_")
        self.assert_compile(
            stmt,
            'INSERT INTO table_1745924 (ca, cb, "_") VALUES (%(ca)s, %(cb)s, %(_)s)',
            dialect="snowflake",
        )

    def test_underscore_as_initial_character_as_non_quoted_identifier(self):
        _table = table(
            "table_1745924",
            column("ca", Integer),
            column("cb", String),
            column("_identifier", String),
        )

        stmt = insert(_table).values(ca=1, cb="test", _identifier="test_")
        self.assert_compile(
            stmt,
            "INSERT INTO table_1745924 (ca, cb, _identifier) VALUES (%(ca)s, %(cb)s, %(_identifier)s)",
            dialect="snowflake",
        )

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


def test_outer_lateral_join():
    col = column("colname").label("label")
    col2 = column("colname2").label("label2")
    lateral_table = func.flatten(func.PARSE_JSON(col2), outer=True).lateral()
    stmt = select(col).select_from(table("abc")).join(lateral_table).group_by(col)
    assert (
        str(stmt.compile(dialect=snowdialect.dialect()))
        == "SELECT colname AS label \nFROM abc JOIN LATERAL flatten(PARSE_JSON(colname2)) AS anon_1 GROUP BY colname"
    )


@pytest.mark.feature_v20
def test_division_operator_with_force_div_is_floordiv_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / col2
    assert (
        str(stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False)))
        == "col1 / col2"
    )


@pytest.mark.feature_v20
def test_division_operator_with_denominator_expr_force_div_is_floordiv_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / func.sqrt(col2)
    assert (
        str(stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False)))
        == "col1 / sqrt(col2)"
    )


@pytest.mark.feature_v20
def test_division_operator_with_force_div_is_floordiv_default_true():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / col2
    assert str(stmt.compile(dialect=SnowflakeDialect())) == "col1 / col2"


@pytest.mark.feature_v20
def test_division_operator_with_denominator_expr_force_div_is_floordiv_default_true():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / func.sqrt(col2)
    assert str(stmt.compile(dialect=SnowflakeDialect())) == "col1 / sqrt(col2)"


@pytest.mark.feature_v20
def test_floor_division_operator_force_div_is_floordiv_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 // col2
    assert (
        str(stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False)))
        == "FLOOR(col1 / col2)"
    )


@pytest.mark.feature_v20
def test_floor_division_operator_with_denominator_expr_force_div_is_floordiv_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 // func.sqrt(col2)
    assert (
        str(stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False)))
        == "FLOOR(col1 / sqrt(col2))"
    )


@pytest.mark.feature_v20
def test_floor_division_operator_force_div_is_floordiv_default_true():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 // col2
    assert str(stmt.compile(dialect=SnowflakeDialect())) == "col1 / col2"


@pytest.mark.feature_v20
def test_floor_division_operator_with_denominator_expr_force_div_is_floordiv_default_true():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 // func.sqrt(col2)
    res = stmt.compile(dialect=SnowflakeDialect())
    assert str(res) == "FLOOR(col1 / sqrt(col2))"
