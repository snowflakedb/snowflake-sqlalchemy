
from snowflake.sqlalchemy import (
    AzureContainer,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    PARQUETFormatter,
)


def test_create_stage(sql_compiler):
    stage = ExternalStage(
        name="AZURE_STAGE",
        namespace="ML_POC.PUBLIC",
    )
    container = AzureContainer(
        account="bysnow",
        container="ml-poc").credentials('saas_token')
    create_stage = CreateStage(stage=stage, container=container)
    actual = sql_compiler(create_stage)
    expected = "CREATE OR REPLACE STAGE ML_POC.PUBLIC.AZURE_STAGE " \
               "URL='azure://bysnow.blob.core.windows.net/ml-poc' " \
               "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    assert actual == expected


def test_create_csv_format(sql_compiler):
    create_format = CreateFileFormat(format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
                                     formatter=CSVFormatter().field_delimiter(","))
    actual = sql_compiler(create_format)
    expected = "CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT TYPE='csv' FIELD_DELIMITER = ','"
    assert actual == expected


def test_create_parquet_format(sql_compiler):
    create_format = CreateFileFormat(format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
                                     formatter=PARQUETFormatter().compression("AUTO"))
    actual = sql_compiler(create_format)
    expected = "CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT TYPE='parquet' COMPRESSION = 'AUTO'"
    assert actual == expected
