#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import json

import pytest
from sqlalchemy import Column, MetaData, Table, text

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
def test_create_table_with_text_type_and_max_lob_size(engine_testaccount):
    metadata = MetaData()
    table_name = "test_max_lob_size_0"
    test_max_lob_size = Table(
        table_name,
        metadata,
        Column("name", TEXT(), primary_key=True),
    )

    metadata.create_all(engine_testaccount)
    try:
        assert test_max_lob_size is not None

        with engine_testaccount.connect() as conn:
            with conn.begin():
                query = text(f"SHOW COLUMNS IN {table_name}")
                result = conn.execute(query)
                row = result.mappings().fetchone()["data_type"]
                type_length = json.loads(row)["length"]
                assert (
                    type_length >= 134217728
                ), f"Expected length to be greater than or equal to 134217728, got {type_length}"

    finally:
        test_max_lob_size.drop(engine_testaccount)
