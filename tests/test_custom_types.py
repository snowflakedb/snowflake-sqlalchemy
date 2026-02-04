#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, text

from snowflake.sqlalchemy import TEXT, custom_types


def test_string_conversions():
    """Makes sure that all of the Snowflake SQLAlchemy types can be turned into Strings"""
    sf_custom_types = [
        "VARIANT",
        "OBJECT",
        "ARRAY",
        "TIMESTAMP_TZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "GEOGRAPHY",
        "GEOMETRY",
    ]
    sf_types = [
        "TEXT",
        "CHARACTER",
        "DEC",
        "DOUBLE",
        "FIXED",
        "NUMBER",
        "BYTEINT",
        "STRING",
        "TINYINT",
        "VARBINARY",
    ] + sf_custom_types

    for type_ in sf_types:
        sample = getattr(custom_types, type_)()
        if type_ in sf_custom_types:
            assert type_ == str(sample)


@pytest.mark.feature_max_lob_size
def test_create_table_with_text_type(engine_testaccount):
    metadata = MetaData()
    table_name = "test_max_lob_size_0"
    test_max_lob_size = Table(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("full_name", TEXT(), server_default=text("id::varchar")),
    )

    metadata.create_all(engine_testaccount)
    try:
        assert test_max_lob_size is not None

        with engine_testaccount.connect() as conn:
            with conn.begin():
                query = text(f"SELECT GET_DDL('TABLE', '{table_name}')")
                result = conn.execute(query)
                row = str(result.mappings().fetchone())
                assert (
                    "VARCHAR(134217728)" in row
                ), f"Expected VARCHAR(134217728) in {row}"

    finally:
        test_max_lob_size.drop(engine_testaccount)
