#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

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


def test_insert_with_identifier():
    metadata = MetaData()
    table = Table(
        "table_1745924",
        metadata,
        Column("ca", Integer),
        Column("cb", String),
        Column("_", String),
    )

    engine = create_engine(URL(**CONNECTION_PARAMETERS))

    try:
        metadata.create_all(engine)

        with engine.connect() as connection:
            connection.execute(insert(table).values(ca=1, cb="test", _="test_"))
            connection.execute(
                insert(table).values({"ca": 2, "cb": "test", "_": "test_"})
            )
            result = connection.execute(select(table)).fetchall()
            assert result == [
                (1, "test", "test_"),
                (2, "test", "test_"),
            ]
    finally:
        metadata.drop_all(engine)
