
from snowflake.sqlalchemy import (
    AzureContainer,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    PARQUETFormatter,
)


def test_create_stage(sql_compiler):
    """
    This test compiles the SQL to create a named stage, by defining the stage naming
    information (namespace and name) and the physical storage information (here: an
    Azure container), and combining them in a CreateStage object
    NB: The test only validates that the correct SQL is generated. It does not
    execute the SQL (yet) against an actual Snowflake instance.
    """
    # define the stage name
    stage = ExternalStage(
        name="AZURE_STAGE",
        namespace="ML_POC.PUBLIC",
    )
    # define the storage container
    container = AzureContainer(
        account="bysnow",
        container="ml-poc"
    ).credentials('saas_token')
    # define the stage object
    create_stage = CreateStage(stage=stage, container=container)

    # validate that the resulting SQL is as expected
    actual = sql_compiler(create_stage)
    expected = "CREATE OR REPLACE STAGE ML_POC.PUBLIC.AZURE_STAGE " \
               "URL='azure://bysnow.blob.core.windows.net/ml-poc' " \
               "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    assert actual == expected


def test_create_csv_format(sql_compiler):
    """
    This test compiles the SQL to create a named CSV format. The format is defined
    using a name and a formatter object with the detailed formatting information.
    TODO: split name parameters into namespace and actual name

    NB: The test only validates that the correct SQL is generated. It does not
    execute the SQL (yet) against an actual Snowflake instance.
    """
    create_format = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=CSVFormatter().field_delimiter(",")
    )
    actual = sql_compiler(create_format)
    expected = "CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT " \
               "TYPE='csv' FIELD_DELIMITER = ','"
    assert actual == expected


def test_create_parquet_format(sql_compiler):
    """
    This test compiles the SQL to create a named Parquet format. The format is defined
    using a name and a formatter object with the detailed formatting information.
    TODO: split name parameters into namespace and actual name

    NB: The test only validates that the correct SQL is generated. It does not
    execute the SQL (yet) against an actual Snowflake instance.

    """
    create_format = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=PARQUETFormatter().compression("AUTO")
    )
    actual = sql_compiler(create_format)
    expected = "CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.CSV_FILE_FORMAT " \
               "TYPE='parquet' COMPRESSION = 'AUTO'"
    assert actual == expected
