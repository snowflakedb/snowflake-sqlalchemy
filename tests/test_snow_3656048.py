#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Regression tests for SNOW-3656048 (COPY / STAGE escaping plus secret
# handling).  Bundles:
#   * SNOW-3649888 — CopyFormatter.value_repr emitted FILE_FORMAT option string
#     values without escaping (single-quote escaping in COPY INTO / CREATE FILE
#     FORMAT).
#   * SNOW-3649782 — AWSBucket/AzureContainer/GCSBucket (and, transitively,
#     CopyIntoStorage) __repr__ logged cloud secrets in plaintext.
#   * SNOW-3649850 — compiled COPY/STAGE SQL inlines cloud secrets, which can
#     surface in the sqlalchemy.engine echo log; mitigated by an opt-in redactor.
#
# Pure-compilation / pure-stdlib tests (no live connection).  Companion to
# tests/test_base_quoting.py (base.py identifier/literal quoting) and
# tests/test_table_option_quoting.py (options compiler-threading).
#
import logging

from sqlalchemy import Column, Integer, MetaData, Table

from snowflake.sqlalchemy import (
    AWSBucket,
    AzureContainer,
    CopyIntoStorage,
    CreateFileFormat,
    CSVFormatter,
    GCSBucket,
    SnowflakeSecretRedactionFilter,
    add_secret_redaction_filter,
    redact_secrets,
)

# A single literal backslash.  "\\" in source is one backslash at runtime.
BS = "\\"


def _src_table():
    meta = MetaData()
    return Table("t", meta, Column("c", Integer))


# ---------------------------------------------------------------------------
# SNOW-3649888 — CopyFormatter.value_repr: FILE_FORMAT option string values
#   must be escaped before being embedded in single-quoted SQL literals.
# ---------------------------------------------------------------------------


def test_snow_3656048_copy_format_option_single_quote_escaping(sql_compiler):
    """A single-quote in a FILE_FORMAT option value must be doubled."""
    value = "YYYY') opt --"
    copy_into = CopyIntoStorage(
        from_=_src_table(),
        into=AWSBucket("backup"),
        formatter=CSVFormatter().date_format(value),
    )
    sql = sql_compiler(copy_into)
    # ' -> '' so the DATE_FORMAT literal is not terminated early.
    assert "DATE_FORMAT='YYYY'') opt --'" in sql
    # The unescaped form must not appear.
    assert "DATE_FORMAT='YYYY') opt --'" not in sql


def test_snow_3656048_create_file_format_option_escaping(sql_compiler):
    """CREATE FILE FORMAT shares the value_repr sink and must escape too."""
    value = "x') ; -- z"
    create_format = CreateFileFormat(
        format_name="MY_FORMAT",
        formatter=CSVFormatter().date_format(value),
    )
    sql = sql_compiler(create_format)
    assert "DATE_FORMAT = 'x'') ; -- z'" in sql


def test_snow_3656048_copy_format_option_backslash_escaping(sql_compiler):
    r"""A backslash before a quote (\') must be neutralised in option values.

    With ESCAPE_STRING_LITERALS=TRUE Snowflake reads \' as an escaped quote; the
    fix doubles the backslash so it sees \\ then ''.
    """
    copy_into = CopyIntoStorage(
        from_=_src_table(),
        into=AWSBucket("backup"),
        formatter=CSVFormatter().time_format("x" + BS + "' --"),
    )
    sql = sql_compiler(copy_into)
    # Python "x\\\\'' --" represents the SQL text: x\\'' --
    assert "TIME_FORMAT='x\\\\'' --'" in sql


def test_snow_3656048_copy_format_single_tuple_escaping(sql_compiler):
    """A single-element option tuple (e.g. NULL_IF) must escape its element."""
    copy_into = CopyIntoStorage(
        from_=_src_table(),
        into=AWSBucket("backup"),
        formatter=CSVFormatter().null_if(["a') --"]),
    )
    sql = sql_compiler(copy_into)
    assert "NULL_IF=('a'') --')" in sql


def test_snow_3656048_copy_format_plain_value_bcr(sql_compiler):
    """BCR: an ordinary option value is emitted unchanged."""
    copy_into = CopyIntoStorage(
        from_=_src_table(),
        into=AWSBucket("backup"),
        formatter=CSVFormatter().date_format("YYYY-MM-DD"),
    )
    sql = sql_compiler(copy_into)
    assert "DATE_FORMAT='YYYY-MM-DD'" in sql


# ---------------------------------------------------------------------------
# SNOW-3649782 — __repr__ must not expose cloud secrets in plaintext.
# ---------------------------------------------------------------------------


def test_snow_3656048_aws_bucket_repr_redacts_credentials():
    bucket = AWSBucket("corp", "path").credentials(
        aws_key_id="AKIAEXAMPLEKEYID",
        aws_secret_key="wJalrXUtSECRETKEY",
        aws_token="SESSIONTOKENVALUE",
    )
    r = repr(bucket)
    assert "wJalrXUtSECRETKEY" not in r
    assert "AKIAEXAMPLEKEYID" not in r
    assert "SESSIONTOKENVALUE" not in r
    assert "AWS_SECRET_KEY='***'" in r
    assert "AWS_KEY_ID='***'" in r
    assert "AWS_TOKEN='***'" in r
    # Non-secret structure stays visible for debugging.
    assert "'s3://corp/path'" in r


def test_snow_3656048_aws_bucket_repr_redacts_master_key():
    bucket = AWSBucket("corp").encryption_aws_cse("MASTERKEYSECRET")
    r = repr(bucket)
    assert "MASTERKEYSECRET" not in r
    assert "MASTER_KEY='***'" in r
    assert "TYPE='AWS_CSE'" in r  # structural, not a secret


def test_snow_3656048_azure_repr_redacts_sas_token():
    container = AzureContainer("acct", "cont").credentials("SASTOKENSECRET")
    r = repr(container)
    assert "SASTOKENSECRET" not in r
    assert "AZURE_SAS_TOKEN='***'" in r


def test_snow_3656048_copy_into_repr_cascades_redaction():
    """CopyIntoStorage.__repr__ must inherit bucket redaction.

    The relevant path is ``CopyInto(from_=bucket, ...)`` whose ``__repr__``
    embeds ``repr(self.from_)`` — the redacted bucket representation.
    """
    bucket = AWSBucket("corp").credentials(
        aws_key_id="AKIAEXAMPLEKEYID", aws_secret_key="wJalrXUtSECRETKEY"
    )
    copy_into = CopyIntoStorage(from_=bucket, into=_src_table())
    r = repr(copy_into)
    assert "wJalrXUtSECRETKEY" not in r
    assert "AKIAEXAMPLEKEYID" not in r


def test_snow_3656048_repr_redaction_does_not_change_compiled_sql(sql_compiler):
    """BCR: compilation still emits the real secret (repr redaction is debug-only)."""
    bucket = AWSBucket("corp").credentials(
        aws_key_id="AKIAEXAMPLEKEYID", aws_secret_key="wJalrXUtSECRETKEY"
    )
    copy_into = CopyIntoStorage(from_=_src_table(), into=bucket)
    sql = sql_compiler(copy_into)
    assert "AWS_SECRET_KEY='wJalrXUtSECRETKEY'" in sql
    assert "AWS_KEY_ID='AKIAEXAMPLEKEYID'" in sql


def test_snow_3656048_gcs_repr_keeps_non_secret_kms_id():
    """BCR: KMS_KEY_ID is a resource id, not a bearer secret — stays visible."""
    bucket = GCSBucket("corp").encryption_gcs_sse_kms("kms-123-abc")
    r = repr(bucket)
    assert "KMS_KEY_ID='kms-123-abc'" in r
    assert "TYPE='GCS_SSE_KMS'" in r


# ---------------------------------------------------------------------------
# SNOW-3649850 — opt-in log redactor for engine logs carrying inline secrets.
# ---------------------------------------------------------------------------

_SAMPLE_SQL = (
    "COPY INTO 's3://corp/backup' FROM t "
    "CREDENTIALS=(AWS_SECRET_KEY='wJalrXUtSECRETKEY' AWS_KEY_ID='AKIAEXAMPLEKEYID') "
    "ENCRYPTION=(TYPE='AWS_CSE' MASTER_KEY='MASTERKEYSECRET')"
)


def test_snow_3656048_redact_secrets_masks_known_keys():
    redacted = redact_secrets(_SAMPLE_SQL)
    assert "wJalrXUtSECRETKEY" not in redacted
    assert "AKIAEXAMPLEKEYID" not in redacted
    assert "MASTERKEYSECRET" not in redacted
    assert "AWS_SECRET_KEY='***'" in redacted
    assert "MASTER_KEY='***'" in redacted
    # Structure and non-secret options are preserved.
    assert "TYPE='AWS_CSE'" in redacted
    assert "COPY INTO 's3://corp/backup' FROM t" in redacted


def test_snow_3656048_redact_secrets_leaves_non_secret_keys():
    text = "CREDENTIALS=(AWS_ROLE='arn:aws:iam::123:role/loader')"
    assert redact_secrets(text) == text


def test_snow_3656048_redact_secrets_handles_doubled_quote():
    text = "AWS_SECRET_KEY='ab''cd'"
    redacted = redact_secrets(text)
    assert redacted == "AWS_SECRET_KEY='***'"


def test_snow_3656048_redact_secrets_handles_backslash():
    # SQL text: MASTER_KEY='a\'b'  (backslash-escaped quote inside the literal)
    text = "MASTER_KEY='a" + BS + "'b'"
    redacted = redact_secrets(text)
    assert redacted == "MASTER_KEY='***'"


def test_snow_3656048_filter_redacts_record_message():
    record = logging.LogRecord(
        name="sqlalchemy.engine.Engine",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=_SAMPLE_SQL,
        args=None,
        exc_info=None,
    )
    assert SnowflakeSecretRedactionFilter().filter(record) is True
    message = record.getMessage()
    assert "wJalrXUtSECRETKEY" not in message
    assert "AWS_SECRET_KEY='***'" in message


def test_snow_3656048_filter_redacts_record_args():
    record = logging.LogRecord(
        name="sqlalchemy.engine.Engine",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="executing %s",
        args=("AWS_SECRET_KEY='wJalrXUtSECRETKEY'",),
        exc_info=None,
    )
    SnowflakeSecretRedactionFilter().filter(record)
    message = record.getMessage()
    assert "wJalrXUtSECRETKEY" not in message
    assert "AWS_SECRET_KEY='***'" in message


def test_snow_3656048_add_filter_to_logger_and_handler():
    logger = logging.getLogger("test_snow_3656048.logger")
    f = add_secret_redaction_filter(logger)
    assert f in logger.filters
    logger.removeFilter(f)

    handler = logging.StreamHandler()
    fh = add_secret_redaction_filter(handler)
    assert fh in handler.filters


def test_snow_3656048_add_filter_rejects_other_targets():
    import pytest

    with pytest.raises(TypeError):
        add_secret_redaction_filter("sqlalchemy.engine")


# ---------------------------------------------------------------------------
# SNOW-3649850 addendum — auto-redaction via SnowflakeDialect.redact_log_secrets
#
# SnowflakeDialect.initialize() (called on first connection) attaches a
# NullHandler carrying SnowflakeSecretRedactionFilter at position 0 on the
# shared "sqlalchemy.engine.Engine" parent logger so that records propagating
# from engine-specific child loggers are redacted before any real handler emits
# them.  Opt-out: create_engine("snowflake://...", redact_log_secrets=False).
# ---------------------------------------------------------------------------

_ENGINE_PARENT_LOGGER_NAME = "sqlalchemy.engine.Engine"


def _count_redaction_null_handlers(logger_name=_ENGINE_PARENT_LOGGER_NAME):
    from snowflake.sqlalchemy.snowdialect import _RedactionHandler

    parent = logging.getLogger(logger_name)
    return sum(
        1
        for h in parent.handlers
        if isinstance(h, _RedactionHandler)
        and any(isinstance(f, SnowflakeSecretRedactionFilter) for f in h.filters)
    )


def _remove_redaction_null_handlers(logger_name=_ENGINE_PARENT_LOGGER_NAME):
    from snowflake.sqlalchemy.snowdialect import _RedactionHandler

    parent = logging.getLogger(logger_name)
    for h in list(parent.handlers):
        if isinstance(h, _RedactionHandler) and any(
            isinstance(f, SnowflakeSecretRedactionFilter) for f in h.filters
        ):
            parent.removeHandler(h)


def test_snow_3656048_ensure_engine_log_redaction_attaches_null_handler():
    from snowflake.sqlalchemy.snowdialect import (
        _ensure_engine_log_redaction,
        _RedactionHandler,
    )

    _remove_redaction_null_handlers()
    try:
        _ensure_engine_log_redaction()
        assert _count_redaction_null_handlers() == 1
        # _RedactionHandler must be first so it runs before any real handler on the logger.
        parent = logging.getLogger(_ENGINE_PARENT_LOGGER_NAME)
        assert isinstance(parent.handlers[0], _RedactionHandler)
    finally:
        _remove_redaction_null_handlers()


def test_snow_3656048_ensure_engine_log_redaction_is_idempotent():
    from snowflake.sqlalchemy.snowdialect import _ensure_engine_log_redaction

    _remove_redaction_null_handlers()
    try:
        _ensure_engine_log_redaction()
        _ensure_engine_log_redaction()
        assert _count_redaction_null_handlers() == 1
    finally:
        _remove_redaction_null_handlers()


def test_snow_3656048_null_handler_redacts_propagated_record():
    """_RedactionHandler filter modifies record.msg in-place before real handlers emit it."""
    from snowflake.sqlalchemy.snowdialect import (
        _ensure_engine_log_redaction,
        _RedactionHandler,
    )

    _remove_redaction_null_handlers()
    try:
        _ensure_engine_log_redaction()
        record = logging.LogRecord(
            name="sqlalchemy.engine.Engine.0xdeadbeef",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg=_SAMPLE_SQL,
            args=None,
            exc_info=None,
        )
        parent = logging.getLogger(_ENGINE_PARENT_LOGGER_NAME)
        redaction_h = next(
            h
            for h in parent.handlers
            if isinstance(h, _RedactionHandler)
            and any(isinstance(f, SnowflakeSecretRedactionFilter) for f in h.filters)
        )
        redaction_h.handle(record)
        assert "wJalrXUtSECRETKEY" not in record.msg
        assert "AWS_SECRET_KEY='***'" in record.msg
    finally:
        _remove_redaction_null_handlers()


def test_snow_3656048_dialect_redact_log_secrets_default_true():
    from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

    dialect = SnowflakeDialect()
    assert dialect._redact_log_secrets is True


def test_snow_3656048_dialect_redact_log_secrets_opt_out():
    from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

    dialect = SnowflakeDialect(redact_log_secrets=False)
    assert dialect._redact_log_secrets is False
