#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    insert,
    select,
)

from snowflake.sqlalchemy import URL

from .parameters import CONNECTION_PARAMETERS

# https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers
DOUBLE_QUOTE_IDENTIFIERS = {i for i in ".'!@#$%^&*"}


@pytest.mark.parametrize(
    "identifier",
    (
        pytest.param(".", id="dot"),
        pytest.param("'", id="single_quote"),
        pytest.param("_", id="underscore"),
        pytest.param("!", id="exclamation"),
        pytest.param("@", id="at"),
        pytest.param("#", id="hash"),
        pytest.param("$", id="dollar"),
        pytest.param("%", id="percent"),
        pytest.param("^", id="caret"),
        pytest.param("&", id="ampersand"),
        pytest.param("*", id="asterisk"),
    ),
)
def test_insert_with_identifier_as_column_name(identifier: str):
    expected_identifier = f"test: {identifier}"
    metadata = MetaData()
    table = Table(
        "table_1745924",
        metadata,
        Column("ca", Integer),
        Column("cb", String),
        Column(identifier, String),
    )

    engine = create_engine(URL(**CONNECTION_PARAMETERS))

    try:
        metadata.create_all(engine)

        with engine.connect() as connection:
            connection.execute(
                insert(table).values(
                    {
                        "ca": 1,
                        "cb": "test",
                        identifier: f"test: {identifier}",
                    }
                )
            )
            result = connection.execute(select(table)).fetchall()
            assert result == [(1, "test", expected_identifier)]
    finally:
        metadata.drop_all(engine)
