#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from unittest import mock

import pytest
from sqlalchemy import MetaData, inspect
from sqlalchemy.sql.ddl import CreateSchema, DropSchema

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from tests.conftest import poll_until
from tests.util import random_string


@pytest.mark.aws
@pytest.mark.parametrize(
    "name_case", [str.upper, str.lower], ids=["uppercase", "lowercase"]
)
@pytest.mark.parametrize(
    "include_columns", [[], ["name3"]], ids=["no_include", "include"]
)
def test_indexes_reflection(
    engine_testaccount, db_parameters, sql_compiler, name_case, include_columns
):
    table_name = "test_hybrid_table_" + random_string(6)
    index_name = name_case("index_name_" + random_string(6))
    schema = db_parameters["schema"]
    index_columns = ["name", "name2"]
    include_sql = f" INCLUDE ({', '.join(include_columns)})" if include_columns else ""

    create_table_sql = f"""
   CREATE HYBRID TABLE {schema}.{table_name} (
        id INT primary key,
        name VARCHAR,
        name2 VARCHAR,
        name3 VARCHAR,
        INDEX {index_name} ({", ".join(index_columns)}){include_sql}
    );
    """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)
        connection.commit()

    try:
        # On AWS, Hybrid Table (Unistore) index metadata can take a few seconds
        # to propagate before SHOW INDEXES IN TABLE reflects the new index.
        indexes = poll_until(
            lambda: inspect(engine_testaccount).get_indexes(table_name, schema),
            timeout=10,
            interval=0.5,
        )

        assert len(indexes) == 1
        assert indexes[0].get("name") == index_name.lower()
        assert indexes[0].get("column_names") == index_columns
        assert indexes[0].get("include_columns") == include_columns

    finally:
        with engine_testaccount.connect() as connection:
            connection.exec_driver_sql(f"DROP TABLE IF EXISTS {schema}.{table_name}")
            connection.commit()


@pytest.mark.aws
def test_simple_reflection_hybrid_table_as_table(
    engine_testaccount, assert_text_in_buf, db_parameters, sql_compiler, snapshot
):
    metadata = MetaData()
    table_name = "test_simple_reflection_hybrid_table_as_table"
    schema = db_parameters["schema"] + "_reflections"
    with engine_testaccount.connect() as connection:
        try:
            connection.execute(CreateSchema(schema))

            create_table_sql = f"""
           CREATE HYBRID TABLE {schema}.{table_name} (id INT primary key, new_column VARCHAR, INDEX index_name (new_column));
            """
            connection.exec_driver_sql(create_table_sql)

            metadata.reflect(engine_testaccount, schema=schema)

            database = db_parameters["database"].upper()
            assert_text_in_buf(
                f'SHOW /* sqlalchemy:get_schema_tables_info */ TABLES IN SCHEMA "{database}"."{schema.upper()}"',
                occurrences=1,
            )

        finally:
            connection.execute(DropSchema(schema, cascade=True))


@pytest.mark.aws
def test_get_multi_indexes_without_filter_names(engine_testaccount, db_parameters):
    schema = db_parameters["schema"] + "_multi_idx_" + random_string(6)
    with engine_testaccount.connect() as connection:
        connection.execute(CreateSchema(schema))
        connection.commit()
    try:
        with engine_testaccount.connect() as connection:
            connection.exec_driver_sql(
                f"CREATE HYBRID TABLE {schema}.ht (id INT PRIMARY KEY, v INT, INDEX ix_v (v))"
            )
            connection.exec_driver_sql(f"CREATE TABLE {schema}.plain (id INT)")
            connection.commit()

        # Hybrid Table index metadata can take a few seconds to propagate.
        ht_indexes = poll_until(
            lambda: (
                inspect(engine_testaccount)
                .get_multi_indexes(schema=schema, filter_names=["ht"])
                .get((schema, "ht"))
            ),
            timeout=10,
            interval=0.5,
        )
        assert [ix["name"] for ix in ht_indexes] == ["ix_v"]

        result = inspect(engine_testaccount).get_multi_indexes(schema=schema)
        assert result == {(schema, "ht"): ht_indexes}
    finally:
        with engine_testaccount.connect() as connection:
            connection.execute(DropSchema(schema, cascade=True))
            connection.commit()


class TestGetMultiIndexes:
    """Unit tests for SnowflakeDialect.get_multi_indexes (no live connection)."""

    IX_HT = [{"name": "ix_v", "unique": False, "column_names": ["v"]}]
    IX_HT2 = [{"name": "ix_w", "unique": True, "column_names": ["w"]}]

    @pytest.fixture
    def dialect(self):
        dialect = SnowflakeDialect()
        dialect.default_schema_name = "public"
        all_indexes = {"ht": self.IX_HT, "ht2": self.IX_HT2, "dropped": []}
        with (
            mock.patch.object(
                SnowflakeDialect,
                "get_table_names_with_prefix",
                return_value=["ht", "ht2"],
            ),
            mock.patch.object(
                SnowflakeDialect, "_get_full_schema_name", return_value='"DB"."S"'
            ),
            mock.patch.object(
                SnowflakeDialect, "_parse_index_rows", return_value=all_indexes
            ),
        ):
            yield dialect

    def test_filter_names_none_returns_all_hybrid_tables(self, dialect):
        result = dialect.get_multi_indexes(
            mock.MagicMock(), schema="s", filter_names=None
        )
        assert sorted(result, key=lambda r: r[0][1]) == [
            (("s", "ht"), self.IX_HT),
            (("s", "ht2"), self.IX_HT2),
        ]

    def test_filter_names_omitted_returns_all_hybrid_tables(self, dialect):
        result = dialect.get_multi_indexes(mock.MagicMock(), schema="s")
        assert {key for key, _ in result} == {("s", "ht"), ("s", "ht2")}

    def test_filter_names_restricts_result(self, dialect):
        result = dialect.get_multi_indexes(
            mock.MagicMock(), schema="s", filter_names=["ht2", "missing"]
        )
        assert result == [(("s", "ht2"), self.IX_HT2)]

    def test_no_hybrid_tables_short_circuits(self):
        dialect = SnowflakeDialect()
        connection = mock.MagicMock()
        with mock.patch.object(
            SnowflakeDialect, "get_table_names_with_prefix", return_value=[]
        ):
            assert dialect.get_multi_indexes(connection, schema="s") == []
        connection.execute.assert_not_called()
