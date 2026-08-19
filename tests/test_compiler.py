#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from datetime import datetime

import pytest
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    and_,
    func,
    insert,
    select,
)
from sqlalchemy.schema import DropColumnComment, DropTableComment
from sqlalchemy.sql import column, quoted_name, table
from sqlalchemy.testing.assertions import AssertsCompiledSQL

from snowflake.sqlalchemy import (
    ARRAY,
    MAP,
    OBJECT,
    VARIANT,
    InsertMulti,
    MergeInto,
    snowdialect,
)
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

    def test_sysdate_func(self):
        statement = select(func.sysdate())
        self.assert_compile(
            statement,
            "SELECT SYSDATE() AS sysdate_1",
            dialect="snowflake",
        )

    def test_now_func(self):
        statement = select(func.now())
        self.assert_compile(
            statement,
            "SELECT CURRENT_TIMESTAMP AS now_1",
            dialect="snowflake",
        )

    def test_collate_order_by(self):
        # Snowflake requires the collation spec as a single-quoted string
        # literal, not a double-quoted identifier (SNOW-629086).
        t = table("some_table", column("id", Integer), column("data", String))
        statement = select(t.c.data).order_by(t.c.data.collate("en-ci").asc())
        self.assert_compile(
            statement,
            "SELECT some_table.data FROM some_table "
            "ORDER BY some_table.data COLLATE 'en-ci' ASC",
            dialect="snowflake",
        )

    def test_collation_in_column_type(self):
        # Column-level collation renders as COLLATE '<spec>' in DDL.
        from sqlalchemy.schema import CreateTable

        metadata = MetaData()
        t = Table(
            "collated_table",
            metadata,
            Column("data", String(100, collation="en-ci")),
        )
        self.assert_compile(
            CreateTable(t),
            "CREATE TABLE collated_table (data VARCHAR(100) COLLATE 'en-ci')",
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

    def test_ilike_compilation(self):
        statement = select(table1.c.name).where(table1.c.name.ilike("%Ann%"))
        self.assert_compile(
            statement,
            "SELECT table1.name FROM table1 WHERE table1.name ILIKE %(name_1)s",
        )

        statement = select(table1.c.name).where(
            ~table1.c.name.ilike("foo\\_%", escape="\\")
        )
        self.assert_compile(
            statement,
            "SELECT table1.name FROM table1 WHERE table1.name NOT ILIKE %(name_1)s ESCAPE '\\\\'",
        )

    def test_regexp_match_with_flags_compilation(self):
        statement = select(table1.c.name).where(
            table1.c.name.regexp_match("ann", flags="i")
        )
        self.assert_compile(
            statement,
            "SELECT table1.name FROM table1 WHERE REGEXP_LIKE(table1.name, %(name_1)s, 'i')",
            dialect="snowflake",
        )

    def test_not_regexp_match_with_flags_compilation(self):
        statement = select(table1.c.name).where(
            ~table1.c.name.regexp_match("ann", flags="i")
        )
        self.assert_compile(
            statement,
            "SELECT table1.name FROM table1 WHERE NOT REGEXP_LIKE(table1.name, %(name_1)s, 'i')",
            dialect="snowflake",
        )

    def test_regexp_replace_with_flags_compilation(self):
        statement = select(table1.c.name.regexp_replace("ann", "bob", flags="i"))
        self.assert_compile(
            statement,
            "SELECT REGEXP_REPLACE(table1.name, %(name_1)s, %(name_2)s, 'i') AS anon_1 FROM table1",
            dialect="snowflake",
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

    def test_offset_without_limit(self):
        self.assert_compile(
            select(table1.c.id).offset(10),
            "SELECT table1.id FROM table1 LIMIT NULL OFFSET %(param_1)s",
            dialect="snowflake",
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


def test_division_operator_with_force_div_is_floordiv_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / col2
    assert (
        str(stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False)))
        == "col1 / col2"
    )


def test_division_operator_with_denominator_expr_force_div_is_floordiv_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / func.sqrt(col2)
    assert (
        str(stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False)))
        == "col1 / sqrt(col2)"
    )


def test_division_operator_force_div_is_floordiv_default_false():
    # New major-release default: force_div_is_floordiv is False, so ``/`` renders
    # as plain true division without the legacy NUMERIC cast.
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / col2
    assert str(stmt.compile(dialect=SnowflakeDialect())) == "col1 / col2"


def test_division_operator_with_denominator_expr_force_div_is_floordiv_default_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / func.sqrt(col2)
    assert str(stmt.compile(dialect=SnowflakeDialect())) == "col1 / sqrt(col2)"


def test_floor_division_operator_force_div_is_floordiv_default_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 // col2
    assert str(stmt.compile(dialect=SnowflakeDialect())) == "FLOOR(col1 / col2)"


def test_floor_division_operator_with_denominator_expr_force_div_is_floordiv_default_false():
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 // func.sqrt(col2)
    res = stmt.compile(dialect=SnowflakeDialect())
    assert str(res) == "FLOOR(col1 / sqrt(col2))"


def test_force_div_is_floordiv_true_is_deprecated_but_still_works():
    # Opting back into the legacy floor-division behaviour must warn, since the
    # default flipped to False in the major release, yet still render the legacy
    # NUMERIC cast for backwards compatibility during the deprecation window.
    col1 = column("col1", Integer)
    col2 = column("col2", Integer)
    stmt = col1 / col2
    with pytest.warns(DeprecationWarning, match="(?i)force_div_is_floordiv"):
        dialect = SnowflakeDialect(force_div_is_floordiv=True)
    assert str(stmt.compile(dialect=dialect)) == "col1 / CAST(col2 AS NUMERIC)"


class TestMergeIntoBindParameters:
    """Regression tests for issue #536: MergeInto must propagate bind variables."""

    target = table("base_table", column("id", Integer), column("ts", DateTime))
    source = table("delta_table", column("id", Integer), column("ts", DateTime))

    def _compile(self, merge):
        dialect = snowdialect.dialect()
        return merge.compile(dialect=dialect)

    def test_merge_into_on_clause_collects_bind_params(self):
        ts_value = datetime(2024, 1, 1)
        merge = MergeInto(
            target=self.target,
            source=self.source,
            on=and_(
                self.target.c.id == self.source.c.id,
                self.target.c.ts >= ts_value,
            ),
        )
        merge.when_matched_then_update().values(id=self.source.c.id)

        compiled = self._compile(merge)
        assert "ts_1" in compiled.params
        assert compiled.params["ts_1"] == ts_value

    def test_merge_into_on_clause_uses_pyformat_placeholder(self):
        merge = MergeInto(
            target=self.target,
            source=self.source,
            on=and_(
                self.target.c.id == self.source.c.id,
                self.target.c.ts >= datetime(2024, 1, 1),
            ),
        )
        merge.when_matched_then_update().values(id=self.source.c.id)

        compiled = self._compile(merge)
        assert "%(ts_1)s" in compiled.string
        assert ":ts_1" not in compiled.string

    def test_merge_into_on_clause_renders_literal_bind(self):
        ts_value = datetime(2024, 1, 1)
        merge = MergeInto(
            target=self.target,
            source=self.source,
            on=and_(
                self.target.c.id == self.source.c.id,
                self.target.c.ts >= ts_value,
            ),
        )
        merge.when_matched_then_update().values(id=self.source.c.id)

        dialect = snowdialect.dialect()
        compiled = merge.compile(
            dialect=dialect, compile_kwargs={"literal_binds": True}
        )
        assert "'2024-01-01 00:00:00.000000'" in compiled.string
        assert "%(ts_1)s" not in compiled.string

    def test_merge_into_where_clause_collects_bind_params(self):
        merge = MergeInto(
            target=self.target,
            source=self.source,
            on=self.target.c.id == self.source.c.id,
        )
        merge.when_matched_then_update().values(id=self.source.c.id).where(
            self.target.c.ts >= datetime(2024, 6, 15)
        )

        compiled = self._compile(merge)
        assert "ts_1" in compiled.params
        assert compiled.params["ts_1"] == datetime(2024, 6, 15)


class TestInsertMulti(AssertsCompiledSQL):
    """Test InsertMulti custom command for multi-table INSERT ALL / INSERT FIRST."""

    __dialect__ = "snowflake"

    target1 = table(
        "target1",
        column("id", Integer),
        column("name", String),
        column("value", Integer),
    )
    target2 = table(
        "target2",
        column("id", Integer),
        column("name", String),
        column("value", Integer),
        schema="test",
    )

    def test_insert_all_unconditional_single_table(self):
        source = select(table1)
        insert_all = InsertMulti(source).into(
            self.target1, columns=["id", "name", "value"]
        )

        dialect = snowdialect.dialect()
        compiled = insert_all.compile(dialect=dialect)
        assert "INSERT ALL" in compiled.string
        assert "INTO target1 (id, name, value)" in compiled.string

    def test_insert_all_unconditional_two_tables(self):
        source = select(table1)
        insert_all = (
            InsertMulti(source)
            .into(self.target1, columns=["id", "name", "value"])
            .into(self.target2, columns=["id", "name", "value"])
        )

        dialect = snowdialect.dialect()
        compiled = insert_all.compile(dialect=dialect)
        assert "INSERT ALL" in compiled.string
        assert "INTO target1 (id, name, value)" in compiled.string
        assert "INTO test.target2 (id, name, value)" in compiled.string


def test_unconditional_insert_all(sql_compiler):
    meta = MetaData()
    users1 = Table(
        "users1",
        meta,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("created_at", DateTime),
    )
    users2 = Table(
        "users2",
        meta,
        Column("id", Integer, Sequence("user_id_seq2"), primary_key=True),
        Column("name", String),
        Column("full/name", String),
    )
    onboarding_users = Table(
        "onboarding_users",
        meta,
        Column("id", Integer, Sequence("new_user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("delete", Boolean),
    )
    insert_all = (
        InsertMulti(
            select(
                onboarding_users.c.id,
                onboarding_users.c.name,
                onboarding_users.c.fullname,
            )
        )
        .into(users1)
        .into(users2)
    )
    assert (
        sql_compiler(insert_all) == "INSERT ALL INTO users1 INTO users2 "
        "SELECT onboarding_users.id AS id, onboarding_users.name AS name, "
        "onboarding_users.fullname AS fullname "
        "FROM onboarding_users"
    )

    stmt = select(
        onboarding_users.c.id,
        onboarding_users.c.name.label("name_label"),
        onboarding_users.c.fullname,
        onboarding_users.c.delete,
    )
    insert_all = (
        InsertMulti(stmt)
        .into(
            users1,
            ["id", "name", users1.c.fullname, users1.c.created_at],
            [
                "id",
                "name_label",
                stmt.selected_columns.fullname,
                func.now(),
            ],
        )
        .into(
            users2,
            [users2.c.name, users2.c["full/name"]],
            [stmt.selected_columns.fullname, stmt.selected_columns.name_label],
        )
    )
    assert (
        sql_compiler(insert_all) == "INSERT ALL "
        "INTO users1 (id, name, fullname, created_at) VALUES (id, name_label, fullname, CURRENT_TIMESTAMP) "
        'INTO users2 (name, "full/name") VALUES (fullname, name_label) '
        "SELECT onboarding_users.id AS id, onboarding_users.name AS name_label, onboarding_users.fullname AS fullname, "
        'onboarding_users."delete" AS "delete" FROM onboarding_users'
    )


def test_conditional_insert_multi(sql_compiler):
    meta = MetaData()
    users1 = Table(
        "users1",
        meta,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
    users2 = Table(
        "users2",
        meta,
        Column("id", Integer, Sequence("user_id_seq2"), primary_key=True),
        Column("name", String),
        Column("full/name", String),
    )
    onboarding_users = Table(
        "onboarding_users",
        meta,
        Column("id", Integer, Sequence("new_user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("delete", Boolean),
    )
    stmt = select(
        onboarding_users.c.id,
        onboarding_users.c.name,
        onboarding_users.c.fullname,
        onboarding_users.c.delete,
    )
    insert_all = (
        InsertMulti(stmt)
        .when(
            stmt.selected_columns.delete,
            users1,
            values=[
                stmt.selected_columns.id,
                stmt.selected_columns.name,
                stmt.selected_columns.fullname,
            ],
        )
        .when(
            ~stmt.selected_columns.delete,
            users2,
            [users2.c.id, users2.c.name, users2.c["full/name"]],
            [
                stmt.selected_columns.id,
                stmt.selected_columns.name,
                stmt.selected_columns.fullname,
            ],
        )
        .else_(users1)
    )
    assert (
        sql_compiler(insert_all) == "INSERT ALL "
        'WHEN "delete" THEN INTO users1 VALUES (id, name, fullname) '
        'WHEN NOT "delete" THEN INTO users2 (id, name, "full/name") VALUES (id, name, fullname) '
        "ELSE INTO users1 "
        "SELECT onboarding_users.id AS id, onboarding_users.name AS name, onboarding_users.fullname AS fullname, "
        'onboarding_users."delete" AS "delete" FROM onboarding_users'
    )


def test_insert_multi_else_requires_when():
    meta = MetaData()
    src = Table("src", meta, Column("id", Integer))
    target = Table("t", meta, Column("id", Integer))
    im = InsertMulti(select(src.c.id))
    with pytest.raises(ValueError):
        im.else_(target)


def test_insert_multi_when_requires_condition():
    meta = MetaData()
    src = Table("src", meta, Column("id", Integer))
    t1 = Table("t1", meta, Column("id", Integer))
    t2 = Table("t2", meta, Column("id", Integer))
    im = InsertMulti(select(src.c.id)).when(src.c.id == 1, t1)
    with pytest.raises(ValueError):
        im.when(None, t2)


def test_insert_multi_not_safe_to_cache():
    # InsertMulti's SQL depends on builder state (clauses/else__/overwrite/first/
    # source) that is not part of any cache-key traversal, so it must opt out of
    # SQLAlchemy statement caching to avoid reusing another statement's SQL.
    assert InsertMulti.inherit_cache is False


def test_insert_multi_repr_has_no_comma_between_targets():
    meta = MetaData()
    src = Table("src", meta, Column("id", Integer))
    t1 = Table("t1", meta, Column("id", Integer))
    t2 = Table("t2", meta, Column("id", Integer))
    im = InsertMulti(select(src.c.id)).into(t1).into(t2)
    text = repr(im)
    # Snowflake separates INTO clauses with whitespace, not commas (matching the
    # compiled SQL); the comma-join artifact ", INTO" must not appear.
    assert ", INTO" not in text
    assert text.count("INTO ") == 2


class TestStructuredTypeGetItem(AssertsCompiledSQL):
    """Subscript access (``col["key"]`` / ``col[index]``) on semi-structured
    columns must compile to Snowflake's native bracket syntax."""

    __dialect__ = "snowflake"

    @staticmethod
    def _table():
        meta = MetaData()
        return Table(
            "semi",
            meta,
            Column("id", Integer),
            Column("va", VARIANT),
            Column("ob", OBJECT),
            Column("ar", ARRAY),
            Column("mp", MAP(String(), String())),
            Column("mp_int", MAP(Integer(), String())),
        )

    def test_object_key_access_literal(self):
        t = self._table()
        self.assert_compile(
            select(t.c.ob["name"]),
            "SELECT semi.ob['name'] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_variant_key_access_literal(self):
        t = self._table()
        self.assert_compile(
            select(t.c.va["a"]),
            "SELECT semi.va['a'] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_array_index_access_literal(self):
        t = self._table()
        self.assert_compile(
            select(t.c.ar[0]),
            "SELECT semi.ar[0] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_nested_object_access_literal(self):
        t = self._table()
        self.assert_compile(
            select(t.c.ob["a"]["b"]),
            "SELECT semi.ob['a']['b'] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_object_key_access_bound_param(self):
        t = self._table()
        self.assert_compile(
            select(t.c.ob["name"]),
            "SELECT semi.ob[%(ob_1)s] AS anon_1 FROM semi",
            dialect="snowflake",
        )

    def test_getitem_in_where_clause(self):
        t = self._table()
        self.assert_compile(
            select(t.c.id).where(t.c.ob["status"] == "active"),
            "SELECT semi.id FROM semi WHERE semi.ob['status'] = 'active'",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_object_key_with_single_quote_is_escaped_literal(self):
        # Keys are rendered by SQLAlchemy's JSON index bind; with literal_binds
        # single quotes are doubled (ANSI/Snowflake string-literal escaping), so
        # a quote-containing key cannot break out of the literal.
        t = self._table()
        self.assert_compile(
            select(t.c.ob["a'b"]),
            "SELECT semi.ob['a''b'] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_object_key_is_bound_not_interpolated(self):
        # Without literal_binds the key is a bound parameter (carried in params,
        # never interpolated into the SQL text) — no injection surface.
        t = self._table()
        self.assert_compile(
            select(t.c.ob["a'; DROP TABLE t;--"]),
            "SELECT semi.ob[%(ob_1)s] AS anon_1 FROM semi",
            dialect="snowflake",
            checkparams={"ob_1": "a'; DROP TABLE t;--"},
        )

    def test_map_key_access_literal(self):
        t = self._table()
        self.assert_compile(
            select(t.c.mp["k"]),
            "SELECT semi.mp['k'] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_array_index_access_bound_param(self):
        # Integer index also renders as a bound parameter by default.
        t = self._table()
        self.assert_compile(
            select(t.c.ar[0]),
            "SELECT semi.ar[%(ar_1)s] AS anon_1 FROM semi",
            dialect="snowflake",
            checkparams={"ar_1": 0},
        )

    def test_getitem_statement_is_cacheable(self):
        # Making the types Indexable must not disable statement caching.
        t = self._table()
        key = select(t.c.id).where(t.c.ob["status"] == "active")._generate_cache_key()
        assert key is not None

    def test_negative_index_compiles_verbatim(self):
        # Snowflake rejects negative array indices at runtime, but the dialect
        # renders whatever index is given; -1 must pass through unchanged (the
        # limitation is the user's responsibility, documented in the README).
        t = self._table()
        self.assert_compile(
            select(t.c.ar[-1]),
            "SELECT semi.ar[-1] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_getitem_in_order_by(self):
        t = self._table()
        self.assert_compile(
            select(t.c.id).order_by(t.c.ob["status"]),
            "SELECT semi.id FROM semi ORDER BY semi.ob['status']",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_getitem_in_group_by(self):
        t = self._table()
        self.assert_compile(
            select(t.c.ob["status"]).group_by(t.c.ob["status"]),
            "SELECT semi.ob['status'] AS anon_1 FROM semi GROUP BY semi.ob['status']",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_map_string_key_access_bound_param(self):
        # MAP with string keys: default (bound) rendering.
        t = self._table()
        self.assert_compile(
            select(t.c.mp["k"]),
            "SELECT semi.mp[%(mp_1)s] AS anon_1 FROM semi",
            dialect="snowflake",
            checkparams={"mp_1": "k"},
        )

    def test_map_integer_key_access_literal(self):
        # MAP with a non-string (integer) key type: numeric keys render verbatim.
        t = self._table()
        self.assert_compile(
            select(t.c.mp_int[1]),
            "SELECT semi.mp_int[1] AS anon_1 FROM semi",
            dialect="snowflake",
            literal_binds=True,
        )

    def test_map_integer_key_access_bound_param(self):
        t = self._table()
        self.assert_compile(
            select(t.c.mp_int[1]),
            "SELECT semi.mp_int[%(mp_int_1)s] AS anon_1 FROM semi",
            dialect="snowflake",
            checkparams={"mp_int_1": 1},
        )
