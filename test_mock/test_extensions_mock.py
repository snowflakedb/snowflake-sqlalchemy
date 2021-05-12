import logging
import re

import pytest
from snowflake.sqlalchemy import (
    URL,
    AzureContainer,
    CopyIntoStorage,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    PARQUETFormatter,
)
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.engine.mock import MockConnection, create_mock_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import select, text

USER_NAME = "foo"
USER_PASSWORD = "foobar"
USER_ACCOUNT = "bar"
DB = "foo_db"
DB_SCHEMA = "foo_schema"
DB_DWH = "foo_dwh"
DB_ROLE = "foo_role"


@pytest.fixture
def url():
    return URL(
        account=USER_ACCOUNT,
        user=USER_NAME,
        password=USER_PASSWORD,
        database=DB,
        schema=DB_SCHEMA,
        warehouse=DB_DWH,
        role=DB_ROLE,
    )


@pytest.fixture
def engine(url):
    def echo_sql(sql, *args, **kwargs):
        """
        This highly sophisticated executor returns the sql query in the selected dialect.
        The only purpose is to check if a sql statement was translated as expected
        """
        if isinstance(sql, str):
            echo = sql
        else:
            echo = sql.compile(dialect=engine.dialect)
        logging.info(f"sql: {echo}")
        return echo

    engine = create_mock_engine(url, echo_sql)
    yield engine
    if not isinstance(engine, MockConnection):
        engine.dispose()


@pytest.fixture
def connection(engine):
    connection = engine.connect()
    yield connection
    if not isinstance(connection, MockConnection):
        connection.close()


def test_select_current_version(connection):
    result = connection.execute("select current_version()")
    assert result == "select current_version()"


def assert_sql(actual, expected):
    actual_mod = re.sub(r"[\n\t]*", "", actual)
    assert actual_mod == expected


def test_create_table(connection, engine):
    metadata = MetaData()
    test_table = Table(
        "test_table",
        metadata,
        Column("int_col", Integer, primary_key=True),
        Column("name_col", String),
    )
    result = CreateTable(test_table)

    assert_sql(
        str(result),
        "CREATE TABLE test_table (int_col INTEGER NOT NULL, name_col VARCHAR, PRIMARY KEY (int_col))",
    )


def test_copy_into_storage_csv(connection, engine):
    metadata = MetaData()
    target_table = Table(
        "TEST_IMPORT",
        metadata,
        Column("COL1", Integer, primary_key=True),
        Column("COL2", String),
    )
    metadata.create_all(engine)

    root_stage = ExternalStage(
        name="AZURE_STAGE",
        namespace="ML_POC.PUBLIC",
    )
    formatter = (
        CSVFormatter()
        .compression("AUTO")
        .field_delimiter(",")
        .record_delimiter(r"\n")
        .field_optionally_enclosed_by(None)
        .escape(None)
        .escape_unenclosed_field(r"\134")
        .date_format("AUTO")
        .null_if([r"\N"])
        .skip_header(1)
        .trim_space(False)
        .error_on_column_count_mismatch(True)
    )
    copy_into = CopyIntoStorage(
        from_=ExternalStage.from_root_stage(root_stage, "testdata"),
        into=target_table,
        formatter=formatter
    )
    copy_into.copy_options = {"pattern": "'.*csv'", "force": "TRUE"}
    result = copy_into.compile(dialect=SnowflakeDialect()).string
    expected = (
        r"COPY INTO TEST_IMPORT "
        r"FROM @ML_POC.PUBLIC.AZURE_STAGE/testdata "
        r"FILE_FORMAT=(TYPE=csv COMPRESSION='auto' FIELD_DELIMITER=',' "
        r"RECORD_DELIMITER='\n' FIELD_OPTIONALLY_ENCLOSED_BY=None "
        r"ESCAPE=None ESCAPE_UNENCLOSED_FIELD='\134' DATE_FORMAT='AUTO' "
        r"NULL_IF=('\N') SKIP_HEADER=1 TRIM_SPACE=False "
        r"ERROR_ON_COLUMN_COUNT_MISMATCH=True) pattern = '.*csv' force = TRUE"
    )
    assert result == expected


def test_copy_into_storage_parquet(connection, engine):
    metadata = MetaData()
    target_table = Table(
        "TEST_IMPORT",
        metadata,
        Column("COL1", Integer, primary_key=True),
        Column("COL2", String),
    )
    metadata.create_all(engine)
    root_stage = ExternalStage(
        name="AZURE_STAGE",
        namespace="ML_POC.PUBLIC",
    )
    sel_statement = select(text("Col1"), text("Col2")).select_from(ExternalStage.from_root_stage(root_stage, "testdata/out.parquet"))

    formatter = PARQUETFormatter().compression("AUTO").binary_as_text(True)

    copy_into = CopyIntoStorage(
        from_=sel_statement,
        into=target_table,
        formatter=formatter
    )
    copy_into.copy_options = {"force": "TRUE"}
    result = copy_into.compile(dialect=SnowflakeDialect()).string
    expected = (
        "COPY INTO TEST_IMPORT "
        "FROM (SELECT Col1, Col2 \n"
        "FROM @ML_POC.PUBLIC.AZURE_STAGE/testdata/out.parquet) "
        "FILE_FORMAT=(TYPE=parquet COMPRESSION='AUTO' BINARY_AS_TEXT=true) force = TRUE"
    )
    assert result == expected


def test_create_stage(connection, engine):
    stage = ExternalStage(
        name="AZURE_STAGE",
        namespace="ML_POC.PUBLIC",
    )
    container = AzureContainer(
        account="bysnow",
        container="ml-poc").credentials('saas_token')
    create_stage = CreateStage(stage=stage, container=container)
    actual = create_stage.compile(dialect=SnowflakeDialect()).string
    expected = "CREATE OR REPLACE STAGE ML_POC.PUBLIC.AZURE_STAGE " \
               "URL='azure://bysnow.blob.core.windows.net/ml-poc' " \
               "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    assert actual == expected
