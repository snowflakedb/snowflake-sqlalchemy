
from snowflake.sqlalchemy import AzureContainer, CreateStage, ExternalStage


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
