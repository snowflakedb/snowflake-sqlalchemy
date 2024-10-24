#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Column, Integer, MetaData, String
from sqlalchemy.exc import ProgrammingError

from snowflake.sqlalchemy import IcebergTable


@pytest.mark.aws
def test_create_iceberg_table(engine_testaccount, snapshot):
    metadata = MetaData()
    external_volume_name = "exvol"
    create_external_volume = f"""
        CREATE OR REPLACE EXTERNAL VOLUME {external_volume_name}
          STORAGE_LOCATIONS =
          (
            (
                NAME = 'my-s3-us-west-2'
                STORAGE_PROVIDER = 'S3'
                STORAGE_BASE_URL = 's3://MY_EXAMPLE_BUCKET/'
                STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/myrole'
                ENCRYPTION=(TYPE='AWS_SSE_KMS' KMS_KEY_ID='1234abcd-12ab-34cd-56ef-1234567890ab')
            )
          );
        """
    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_external_volume)
    IcebergTable(
        "Iceberg_Table_1",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("geom", String),
        external_volume=external_volume_name,
        base_location="my_iceberg_table",
    )

    with pytest.raises(ProgrammingError) as argument_error:
        metadata.create_all(engine_testaccount)

    error_str = str(argument_error.value)
    assert error_str[: error_str.rfind("\n")] == snapshot
