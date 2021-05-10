import logging
import re

import pytest
from snowflake.sqlalchemy import URL, AzureContainer, CopyIntoStorage, CSVFormatter, ExternalStage, PARQUETFormatter
from sqlalchemy import Column, Integer, MetaData, String, Table
from sqlalchemy.engine.mock import MockConnection, create_mock_engine
from sqlalchemy.schema import CreateTable

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


def test_copy_into_storage(connection, engine):
    metadata = MetaData()
    target_table = Table(
        "TEST_IMPORT",
        metadata,
        Column("COL1", Integer, primary_key=True),
        Column("COL2", String),
    )
    metadata.create_all(engine)

    src_container = AzureContainer.from_uri(
        "azure://bysnow.blob.core.windows.net/ml-poc/testdata"
    )
    # workaround for a bug
    src_container.credentials_used = {}
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
        from_=src_container, into=target_table, formatter=formatter
    )
    copy_into.copy_options = {"pattern": "'.*csv'", "force": "TRUE"}
    result = copy_into.__repr__()
    expected = (
        r"COPY INTO TEST_IMPORT "
        r"FROM 'azure://bysnow.blob.core.windows.net/ml-poc/testdata' "
        r"FILE_FORMAT=(TYPE=csv COMPRESSION = 'auto' FIELD_DELIMITER = ',' "
        r"RECORD_DELIMITER = '\n' FIELD_OPTIONALLY_ENCLOSED_BY = None "
        r"ESCAPE = None ESCAPE_UNENCLOSED_FIELD = '\134' DATE_FORMAT = 'AUTO' "
        r"NULL_IF = ('\N') SKIP_HEADER = 1 TRIM_SPACE = False "
        r"ERROR_ON_COLUMN_COUNT_MISMATCH = True) pattern = '.*csv' force = TRUE"
    )
    assert result == expected


@pytest.mark.xfail
def test_create_csv_format(connection, engine):
    formatter = (
        CSVFormatter()
        .compression("AUTO")
        .field_delimiter(",")
        .record_delimiter("\n")
        .field_optionally_enclosed_by(None)
        .escape(None)
        .escape_unenclosed_field(r"\134")
        .date_format("AUTO")
        .null_if("\\N")
    )
    # sql = formatter.compile()
    assert repr(formatter) == "expected command here"


@pytest.mark.xfail
def test_create_parquet_format(connection, engine):
    formatter = PARQUETFormatter()
    assert repr(formatter) == "expected command here"


@pytest.mark.xfail
def test_create_stage(connection, engine):
    stage = ExternalStage(
        "AZURE_STAGE",
        path="azure://bysnow.blob.core.windows.net/ml-poc/",
        namespace="ML_POC.PUBLIC",
    )
    rep = repr(stage)
    assert rep == "expected command here"
