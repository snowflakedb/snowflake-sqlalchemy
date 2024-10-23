#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, insert, select


@pytest.mark.parametrize(
    "identifier",
    (
        pytest.param("_", id="underscore"),
        pytest.param(".", id="dot"),
    ),
)
def test_insert_with_identifier_as_column_name(identifier: str, engine_testaccount):
    expected_identifier = f"test: {identifier}"
    metadata = MetaData()
    table = Table(
        "table_1745924",
        metadata,
        Column("ca", Integer),
        Column("cb", String),
        Column(identifier, String),
    )

    try:
        metadata.create_all(engine_testaccount)

        with engine_testaccount.connect() as connection:
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
        metadata.drop_all(engine_testaccount)
