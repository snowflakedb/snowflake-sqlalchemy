#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy.exc import ProgrammingError

from snowflake.sqlalchemy import IcebergTable


@pytest.mark.aws
def test_create_iceberg_table(engine_testaccount, external_volume):
    metadata = MetaData()
    IcebergTable(
        "Iceberg_Table_1",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        external_volume=external_volume,
        base_location="my_iceberg_table",
    )

    with pytest.raises(ProgrammingError) as argument_error:
        metadata.create_all(engine_testaccount)

    error_str = str(argument_error.value)
    assert (
        "(snowflake.connector.errors.ProgrammingError)"
        in error_str[: error_str.rfind("\n")]
    )
