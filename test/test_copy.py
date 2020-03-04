#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#
import pytest
from snowflake.sqlalchemy import (
    AWSBucket,
    AzureContainer,
    CopyIntoStorage,
    CSVFormatter,
    JSONFormatter,
    PARQUETFormatter,
    ExternalStage
)
from sqlalchemy import Column, Integer, MetaData, Sequence, String, Table
from sqlalchemy.sql import select


def test_external_stage():
    assert ExternalStage.prepare_namespace("something") == "something."
    assert ExternalStage.prepare_path("prefix") == "/prefix"

    # All arguments are handled
    assert (
        str(ExternalStage(name="name", path="prefix/path", namespace="namespace") == "@namespace.name/prefix/path"
    )

    # defaults don't ruin things
    assert str(ExternalStage(name="name", path=None, namespace=None)) == "@name"


def test_copy_into_location(engine_testaccount, sql_compiler):
    meta = MetaData()
    conn = engine_testaccount.connect()
    food_items = Table("python_tests_foods", meta,
                       Column('id', Integer, Sequence('new_user_id_seq'), primary_key=True),
                       Column('name', String),
                       Column('quantity', Integer))
    meta.create_all(engine_testaccount)
    copy_stmt_1 = CopyIntoStorage(from_=food_items,
                                  into=AWSBucket.from_uri('s3://backup').encryption_aws_sse_kms(
                                      '1234abcd-12ab-34cd-56ef-1234567890ab'),
                                  formatter=CSVFormatter().record_delimiter('|').escape(None).null_if(['null', 'Null']))
    assert (sql_compiler(copy_stmt_1) == "COPY INTO 's3://backup' FROM python_tests_foods FILE_FORMAT=(TYPE=csv "
                                         "ESCAPE=None NULL_IF=('null', 'Null') RECORD_DELIMITER='|') ENCRYPTION="
                                         "(KMS_KEY_ID='1234abcd-12ab-34cd-56ef-1234567890ab' TYPE='AWS_SSE_KMS')")
    copy_stmt_2 = CopyIntoStorage(from_=select([food_items]).where(food_items.c.id == 1),  # Test sub-query
                                  into=AWSBucket.from_uri('s3://backup').credentials(
                                      aws_role='some_iam_role').encryption_aws_sse_s3(),
                                  formatter=JSONFormatter().file_extension('json').compression('zstd'))
    assert (sql_compiler(copy_stmt_2) == "COPY INTO 's3://backup' FROM (SELECT python_tests_foods.id, "
                                         "python_tests_foods.name, python_tests_foods.quantity FROM python_tests_foods "
                                         "WHERE python_tests_foods.id = 1) FILE_FORMAT=(TYPE=json COMPRESSION='zstd' "
                                         "FILE_EXTENSION='json') CREDENTIALS=(AWS_ROLE='some_iam_role') "
                                         "ENCRYPTION=(TYPE='AWS_SSE_S3')")
    copy_stmt_3 = CopyIntoStorage(from_=food_items,
                                  into=AzureContainer.from_uri(
                                      'azure://snowflake.blob.core.windows.net/snowpile/backup'
                                  ).credentials('token'),
                                  formatter=PARQUETFormatter().snappy_compression(True))
    assert (sql_compiler(copy_stmt_3) == "COPY INTO 'azure://snowflake.blob.core.windows.net/snowpile/backup' "
                                         "FROM python_tests_foods FILE_FORMAT=(TYPE=parquet SNAPPY_COMPRESSION=true) "
                                         "CREDENTIALS=(AZURE_SAS_TOKEN='token')")

    copy_stmt_3.maxfilesize(50000000)
    assert (sql_compiler(copy_stmt_3) == "COPY INTO 'azure://snowflake.blob.core.windows.net/snowpile/backup' "
                                         "FROM python_tests_foods FILE_FORMAT=(TYPE=parquet SNAPPY_COMPRESSION=true) "
                                         "MAX_FILE_SIZE = 50000000 "
                                         "CREDENTIALS=(AZURE_SAS_TOKEN='token')")

    copy_stmt_4 = CopyIntoStorage(from_=AWSBucket.from_uri('s3://backup').encryption_aws_sse_kms(
        '1234abcd-12ab-34cd-56ef-1234567890ab'),
        into=food_items,
        formatter=CSVFormatter().record_delimiter('|').escape(None).null_if(['null', 'Null']))
    assert (sql_compiler(copy_stmt_4) == "COPY INTO python_tests_foods FROM 's3://backup' FILE_FORMAT=(TYPE=csv "
                                         "ESCAPE=None NULL_IF=('null', 'Null') RECORD_DELIMITER='|') ENCRYPTION="
                                         "(KMS_KEY_ID='1234abcd-12ab-34cd-56ef-1234567890ab' TYPE='AWS_SSE_KMS')")

    copy_stmt_5 = CopyIntoStorage(from_=AWSBucket.from_uri('s3://backup').encryption_aws_sse_kms(
        '1234abcd-12ab-34cd-56ef-1234567890ab'),
        into=food_items,
        formatter=CSVFormatter().field_delimiter(','))
    assert (sql_compiler(copy_stmt_5) == "COPY INTO python_tests_foods FROM 's3://backup' FILE_FORMAT=(TYPE=csv "
                                         "FIELD_DELIMITER=',') ENCRYPTION="
                                         "(KMS_KEY_ID='1234abcd-12ab-34cd-56ef-1234567890ab' TYPE='AWS_SSE_KMS')")

    copy_stmt_6 = CopyIntoStorage(from_=food_items, into=ExternalStage(name="stage_name"), formatter=CSVFormatter())
    assert sql_compiler(copy_stmt_6) == "COPY INTO @stage_name FROM python_test_foods FILE_FORMAT=(TYPE=csv)"

    copy_stmt_7 = CopyIntoStorage(from_=food_items, into=ExternalStage(name="stage_name", path="prefix/file", namespace="name"), formatter=CSVFormatter())
    assert sql_compiler(copy_stmt_7) == "COPY INTO @name.stage_name/prefix/file FROM python_test_foods FILE_FORMAT=(TYPE=csv)"

    # NOTE Other than expect known compiled text, submit it to RegressionTests environment and expect them to fail, but
    # because of the right reasons
    try:
        acceptable_exc_reasons = {'Failure using stage area',
                                  'AWS_ROLE credentials are not allowed for this account.',
                                  'AWS_ROLE credentials are invalid'}
        for stmnt in (copy_stmt_1, copy_stmt_2, copy_stmt_3, copy_stmt_4):
            with pytest.raises(Exception) as exc:
                conn.execute(stmnt)
            if not any(map(lambda reason: reason in str(exc) or reason in str(exc.value), acceptable_exc_reasons)):
                raise Exception("Not acceptable exception: {} {}".format(str(exc), str(exc.value)))
    finally:
        conn.close()
        food_items.drop(engine_testaccount)
