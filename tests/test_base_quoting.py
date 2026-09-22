#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# Regression tests for SNOW-3656026 (base.py identifier and literal
# quoting).  Anchored by SNOW-3649763 and bundling SNOW-3649858,
# SNOW-3649881, SNOW-3649816, SNOW-3649808, SNOW-3649824.
#
# These are pure-compilation tests (no live connection): each builds a
# construct and asserts on the SQL emitted by the Snowflake dialect.  Tests
# named ``*_quoting`` / ``*_escaping`` encode the expected behaviour and are
# expected to FAIL until the base.py fixes land (TDD red state).  Tests named
# ``*_bcr`` / ``*_preserved`` lock down behaviour that must NOT change and are
# expected to pass both before and after the fix.
#
# Companion to tests/test_table_option_quoting.py
# (options compiler-threading) and tests/test_unit_escaping.py (util helpers).
#
import pytest
from sqlalchemy import Boolean, Column, Integer, MetaData, Sequence, String, Table
from sqlalchemy.sql import select, text
from sqlalchemy.sql.elements import quoted_name

from snowflake.sqlalchemy import (
    AWSBucket,
    AzureContainer,
    CopyFormatter,
    CopyIntoStorage,
    CreateFileFormat,
    CreateStage,
    CSVFormatter,
    ExternalStage,
    GCSBucket,
    MergeInto,
)

# A single literal backslash.  "\\" in source is one backslash at runtime.
BS = "\\"


def _merge_tables():
    meta = MetaData()
    users = Table(
        "users",
        meta,
        Column("id", Integer, Sequence("user_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
    )
    staging = Table(
        "staging_users",
        meta,
        Column("id", Integer, Sequence("staging_id_seq"), primary_key=True),
        Column("name", String),
        Column("fullname", String),
        Column("delete", Boolean),
    )
    return users, staging


# ---------------------------------------------------------------------------
# SNOW-3649763 — MergeInto: unquoted column keys in WHEN NOT MATCHED
#   THEN INSERT (...) and in WHEN MATCHED THEN UPDATE SET allow identifier
#   quoting.  base.py:584 (INSERT column list) and base.py:594 (UPDATE SET).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "apply_op,value",
    [
        pytest.param(
            lambda m, s, p: m.when_not_matched_then_insert().values(**{p: s.c.name}),
            "name) -- x",
            id="insert_column_key",
        ),
        pytest.param(
            lambda m, s, p: m.when_matched_then_update().values(**{p: s.c.name}),
            "name = 1 -- x",
            id="update_set_key",
        ),
    ],
)
def test_snow_3656026_merge_key_quoting(sql_compiler, apply_op, value):
    """Column keys must be identifier-quoted in both INSERT and UPDATE SET sinks."""
    users, staging = _merge_tables()
    merge = MergeInto(users, staging, users.c.id == staging.c.id)
    apply_op(merge, staging, value)
    sql = sql_compiler(merge)
    assert f'"{value}"' in sql


def test_snow_3656026_merge_plain_keys_bcr(sql_compiler):
    """BCR: ordinary column keys stay bare (no regression vs. current output)."""
    users, staging = _merge_tables()
    merge = MergeInto(users, staging, users.c.id == staging.c.id)
    merge.when_not_matched_then_insert().values(
        id=staging.c.id, name=staging.c.name, fullname=staging.c.fullname
    )
    sql = sql_compiler(merge)
    assert "INSERT (fullname, id, name) VALUES" in sql
    assert '"name"' not in sql  # legal identifiers are not force-quoted


# ---------------------------------------------------------------------------
# SNOW-3649808 — quote_schema()/_quote_free_identifiers emit raw
#   schema when an application passes quoted_name(<value>, quote=False).
#   base.py:468.
# ---------------------------------------------------------------------------


def test_snow_3656026_quote_schema_quote_false_quoting(sql_compiler):
    """A quote=False schema that is unsafe must still be quoted, not emitted raw."""
    value = "PUB; -- x"
    meta = MetaData()
    table = Table(
        "t",
        meta,
        Column("c", Integer),
        schema=quoted_name(value, quote=False),
    )
    sql = sql_compiler(select(table))
    # After fix: schema is wrapped as a single quoted identifier (inert),
    # rather than splat verbatim into the FROM clause.
    assert f'"{value}"' in sql
    assert f"FROM {value}" not in sql


def test_snow_3656026_quote_schema_dotted_value_is_split_not_raw(sql_compiler):
    """A dotted quote=False value is parsed as db.schema, not parsed raw.

    Deliberate non-goal: ``quoted_name("OTHER_DB.OTHER_SCHEMA", quote=False)`` is
    split on the dot and each (legal) part is emitted bare, which is the correct
    interpretation of a qualified schema — indistinguishable from a genuine
    ``db.schema`` reference, so preventing cross-tenant *redirection* is an
    application-level validation concern.  The fix only guarantees no SQL
    *syntax* is emitted: the output stays a well-formed dotted identifier.
    """
    value = "OTHER_DB.OTHER_SCHEMA"
    meta = MetaData()
    table = Table(
        "t",
        meta,
        Column("c", Integer),
        schema=quoted_name(value, quote=False),
    )
    sql = sql_compiler(select(table))
    assert "FROM OTHER_DB.OTHER_SCHEMA.t" in sql
    assert ";" not in sql and "--" not in sql


def test_snow_3656026_quote_schema_safe_value_bcr(sql_compiler):
    """BCR: a legal bare schema with quote=False stays unquoted (documented idiom)."""
    meta = MetaData()
    table = Table(
        "report",
        meta,
        Column("c", Integer),
        schema=quoted_name("TENANT1", quote=False),
    )
    sql = sql_compiler(select(table))
    assert "TENANT1.report" in sql
    assert '"TENANT1"' not in sql


# ---------------------------------------------------------------------------
# SNOW-3649824 — format_label returns the raw label when it is a
#   quoted_name with quote=False, enabling single-statement projection-list
#   quoting.  base.py:484.
# ---------------------------------------------------------------------------


def test_snow_3656026_format_label_quote_false_quoting(sql_compiler):
    """An unsafe quote=False column alias must be quoted, not emitted raw."""
    value = "x, y -- z"
    meta = MetaData()
    table = Table("t", meta, Column("c", Integer))
    sql = sql_compiler(select(table.c.c.label(quoted_name(value, quote=False))))
    # After the fix the alias is a single quoted identifier, so a comma in the
    # value cannot split it into multiple projection items.  Assert the unquoted
    # comma form does not appear.
    assert f'"{value}"' in sql
    assert "AS x, y" not in sql


def test_snow_3656026_format_label_safe_alias_bcr(sql_compiler):
    """BCR: a legal bare alias with quote=False stays unquoted."""
    meta = MetaData()
    table = Table("t", meta, Column("c", Integer))
    sql = sql_compiler(select(table.c.c.label(quoted_name("myalias", quote=False))))
    assert "AS myalias" in sql
    assert '"myalias"' not in sql


# ---------------------------------------------------------------------------
# SNOW-3649881 — format_name interpolated raw at three sinks:
#   visit_copy_formatter (base.py:674), visit_create_file_format (base.py:1119),
#   visit_external_stage (base.py:773).  format_name is a (possibly
#   schema-qualified) identifier and must be routed through the preparer.
# ---------------------------------------------------------------------------


def test_snow_3656026_copy_formatter_format_name_quoting(sql_compiler):
    """visit_copy_formatter: format_name must be identifier-quoted."""
    value = "fmt) opt --"
    meta = MetaData()
    target = Table("t", meta, Column("c", Integer))
    stage = ExternalStage(name="S", namespace="DB.SCH")
    copy_into = CopyIntoStorage(
        from_=stage, into=target, formatter=CopyFormatter(format_name=value)
    )
    sql = sql_compiler(copy_into)
    assert f'"{value}"' in sql


def test_snow_3656026_create_file_format_name_quoting(sql_compiler):
    """visit_create_file_format: format_name must be identifier-quoted."""
    value = "fmt TYPE='csv' -- x"
    create_format = CreateFileFormat(
        format_name=value, formatter=CSVFormatter().field_delimiter(",")
    )
    sql = sql_compiler(create_format)
    assert f'"{value}"' in sql


def test_snow_3656026_external_stage_file_format_quoting(sql_compiler):
    """visit_external_stage: file_format must be identifier-quoted."""
    value = "fmt) x --"
    stage = ExternalStage(name="S", namespace="DB.SCH", file_format=value)
    sql = sql_compiler(stage)
    assert f'"{value}"' in sql


def test_snow_3656026_format_name_qualified_bcr(sql_compiler):
    """BCR: a legal (schema-qualified) format name stays bare."""
    create_format = CreateFileFormat(
        format_name="ML_POC.PUBLIC.CSV_FILE_FORMAT",
        formatter=CSVFormatter().field_delimiter(","),
    )
    sql = sql_compiler(create_format)
    assert "ML_POC.PUBLIC.CSV_FILE_FORMAT" in sql
    assert '"ML_POC"' not in sql


# ---------------------------------------------------------------------------
# SNOW-3649816 — AWS/Azure/GCS bucket path & credential single-quote
#   escaping in COPY INTO string literals.  base.py:702/707/713 (AWS),
#   727/732/738 (Azure), 753/759 (GCS).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bucket_cls", [AWSBucket, GCSBucket], ids=["aws", "gcs"])
def test_snow_3656026_bucket_path_quote_escaping(sql_compiler, bucket_cls):
    """Bucket path: an embedded single-quote must be doubled (AWS and GCS)."""
    meta = MetaData()
    src = Table("t", meta, Column("c", Integer))
    bucket = bucket_cls("corp-bucket", "x' STORAGE_INTEGRATION=other_int --")
    copy_into = CopyIntoStorage(from_=src, into=bucket)
    sql = sql_compiler(copy_into)
    assert "x'' STORAGE_INTEGRATION=other_int --" in sql


def test_snow_3656026_aws_bucket_credential_quote_escaping(sql_compiler):
    """AWS CREDENTIALS value: an embedded single-quote must be doubled."""
    meta = MetaData()
    src = Table("t", meta, Column("c", Integer))
    bucket = AWSBucket("corp-bucket", "p").credentials(aws_role="r' XX --")
    copy_into = CopyIntoStorage(from_=src, into=bucket)
    sql = sql_compiler(copy_into)
    assert "AWS_ROLE='r'' XX --'" in sql


def test_snow_3656026_aws_bucket_path_backslash_escaping(sql_compiler):
    r"""AWS bucket path: a backslash before a quote (\') must be neutralised.

    With ESCAPE_STRING_LITERALS=TRUE Snowflake reads \' as an escaped quote;
    the fix doubles the backslash so it sees \\ then ''.
    """
    meta = MetaData()
    src = Table("t", meta, Column("c", Integer))
    bucket = AWSBucket("corp-bucket", "p" + BS + "' escaping --")
    copy_into = CopyIntoStorage(from_=src, into=bucket)
    sql = sql_compiler(copy_into)
    # Python "\\\\'' escaping" represents the SQL text: \\'' escaping
    assert "\\\\'' escaping --" in sql


def test_snow_3656026_azure_sas_token_quote_escaping(sql_compiler):
    """Azure SAS token: an embedded single-quote must be doubled."""
    meta = MetaData()
    src = Table("t", meta, Column("c", Integer))
    container = AzureContainer("acct", "cont").credentials(
        "?sv=2020' ) STORAGE_INTEGRATION=other_int --"
    )
    copy_into = CopyIntoStorage(from_=src, into=container)
    sql = sql_compiler(copy_into)
    assert "AZURE_SAS_TOKEN='?sv=2020'' ) STORAGE_INTEGRATION=other_int --'" in sql


def test_snow_3656026_aws_bucket_plain_value_bcr(sql_compiler):
    """BCR: a bucket path without special characters is emitted unchanged."""
    meta = MetaData()
    src = Table("t", meta, Column("c", Integer))
    bucket = AWSBucket("backup", "subdir/path")
    copy_into = CopyIntoStorage(from_=src, into=bucket)
    sql = sql_compiler(copy_into)
    assert "'s3://backup/subdir/path'" in sql


# ---------------------------------------------------------------------------
# SNOW-3649858 — CreateStage: stage namespace/name interpolated
#   without identifier quoting and container rendered via repr() without
#   single-quote escaping.  base.py:1104.
# ---------------------------------------------------------------------------


def test_snow_3656026_create_stage_name_quoting(sql_compiler):
    """CreateStage: an unsafe stage name must be identifier-quoted."""
    value = "S URL='s3://b/' -- x"
    stage = ExternalStage(name=value, namespace="DB.SCH")
    container = AzureContainer("acct", "cont").credentials("tok")
    create_stage = CreateStage(stage=stage, container=container)
    sql = sql_compiler(create_stage)
    assert f'"{value}"' in sql


def test_snow_3656026_create_stage_container_quote_escaping(sql_compiler):
    """CreateStage: a single-quote in the container URL must be doubled.

    visit_create_stage currently renders the URL via repr(container); the fix
    must escape the literal regardless of how the URL is produced.
    """
    stage = ExternalStage(name="S", namespace="DB.SCH")
    container = AzureContainer("acct", "cont", "p' -- x")
    create_stage = CreateStage(stage=stage, container=container)
    sql = sql_compiler(create_stage)
    assert "p'' -- x" in sql


def test_snow_3656026_create_stage_plain_bcr(sql_compiler):
    """BCR: a well-formed CreateStage compiles exactly as it does today."""
    stage = ExternalStage(name="AZURE_STAGE", namespace="MY_DB.MY_SCHEMA")
    container = AzureContainer("myaccount", "my-container").credentials("saas_token")
    create_stage = CreateStage(stage=stage, container=container)
    sql = sql_compiler(create_stage)
    expected = (
        "CREATE STAGE MY_DB.MY_SCHEMA.AZURE_STAGE "
        "URL='azure://myaccount.blob.core.windows.net/my-container' "
        "CREDENTIALS=(AZURE_SAS_TOKEN='saas_token')"
    )
    assert sql == expected


# ---------------------------------------------------------------------------
# SNOW-3649871 — FilesOption.__str__ used \' escaping (no
#   backslash doubling), allowing a backslash-before-quote escaping from
#   FILES=('...').  custom_commands.py:106.
# ---------------------------------------------------------------------------


def _copy_with_files(file_names):
    meta = MetaData()
    src = Table("t", meta, Column("c", Integer))
    bucket = AWSBucket("backup")
    copy_into = CopyIntoStorage(from_=src, into=bucket)
    copy_into.files(file_names)
    return copy_into


def test_snow_3656026_files_single_quote_quoting(sql_compiler):
    """FILES: an embedded single-quote must be doubled, not backslash-escaped."""
    copy_into = _copy_with_files(["x') PURGE = TRUE FORCE = TRUE --"])
    sql = sql_compiler(copy_into)
    assert "'x'') PURGE = TRUE FORCE = TRUE --'" in sql
    assert "\\'" not in sql  # old \' convention must be gone


def test_snow_3656026_files_backslash_escaping(sql_compiler):
    r"""FILES: a backslash before a quote (\') must be neutralised.

    Old output for ``x\') ... `` was ``'x\\') ...'`` — Snowflake reads ``\\`` as
    one literal backslash, leaving ``'`` to terminate the literal early.  The
    fix doubles both: ``\`` -> ``\\`` and ``'`` -> ``''``.
    """
    copy_into = _copy_with_files(["x" + BS + "') PURGE = TRUE --"])
    sql = sql_compiler(copy_into)
    # Python "x\\\\'') ..." represents the SQL text: x\\'') ...
    assert "'x\\\\'') PURGE = TRUE --'" in sql


def test_snow_3656026_files_plain_value_bcr(sql_compiler):
    """BCR: ordinary file names are emitted unchanged inside FILES=(...)."""
    copy_into = _copy_with_files(["data1.csv", "data2.csv"])
    sql = sql_compiler(copy_into)
    assert "FILES = ('data1.csv','data2.csv')" in sql


class TestSnow4134196ExternalStagePathQuoting:
    """SNOW-4134196 — ``visit_external_stage`` stage-path quoting (CWE-89).

    ``visit_external_stage`` interpolated the application-supplied
    ``ExternalStage.path`` verbatim after the (identifier-quoted) stage name.
    Because whitespace / quotes / parens / ``--`` end the bare stage reference,
    such a path could change the surrounding COPY grammar — e.g. redirect a
    privileged COPY INTO or add PATTERN/FORCE/PURGE.  The fix single-quotes
    (and escapes) the whole reference for any unsafe path, so the path can no
    longer break out.  Builds on the SNOW-3656026 quoting/escaping helpers
    exercised above.
    """

    def test_snow_4134196_external_stage_path_quoting(self, sql_compiler):
        """An unsafe stage path must be wrapped in a single-quoted literal."""
        path = "out FROM (SELECT secret FROM sensitive) OVERWRITE=TRUE --"
        stage = ExternalStage(name="EXPORTS", namespace="DB.SCH", path=path)
        sql = sql_compiler(stage)
        # The entire @<stage>/<path> reference is a single string literal, so
        # nothing in the path escapes into COPY grammar.
        expected = "'@DB.SCH.EXPORTS/out FROM (SELECT secret FROM sensitive) OVERWRITE=TRUE --'"
        assert sql == expected

    def test_snow_4134196_external_stage_path_in_copy_into_unload(self, sql_compiler):
        """A COPY INTO unload target path cannot break out into COPY grammar."""
        meta = MetaData()
        src = Table("t", meta, Column("c", Integer))
        path = "out FROM (SELECT secret FROM sensitive) OVERWRITE=TRUE --"
        stage = ExternalStage(name="EXPORTS", namespace="DB.SCH", path=path)
        copy_into = CopyIntoStorage(from_=src, into=stage)
        sql = sql_compiler(copy_into)
        # The stage target is a quoted literal; the statement's real FROM is the
        # table, not the subquery spelled out in the path.
        expected_prefix = "COPY INTO '@DB.SCH.EXPORTS/out FROM (SELECT secret FROM sensitive) OVERWRITE=TRUE --' FROM t"
        assert sql.startswith(expected_prefix)

    def test_snow_4134196_external_stage_path_in_copy_into_import(self, sql_compiler):
        """A COPY INTO import source path cannot add PATTERN/FORCE/PURGE."""
        meta = MetaData()
        target = Table("t", meta, Column("c", Integer))
        path = "in PATTERN='.*' FORCE=TRUE PURGE=TRUE --"
        stage = ExternalStage(name="IMPORTS", namespace="DB.SCH", path=path)
        copy_into = CopyIntoStorage(from_=stage, into=target)
        sql = sql_compiler(copy_into)
        # The stage source is a single quoted literal; no bare PATTERN/FORCE/PURGE
        # options appear in the statement outside that literal.
        expected = (
            "COPY INTO t FROM "
            "'@DB.SCH.IMPORTS/in PATTERN=''.*'' FORCE=TRUE PURGE=TRUE --'   "
        )
        assert sql == expected

    def test_snow_4134196_external_stage_path_in_select_from(self, sql_compiler):
        """A staged-data SELECT ... FROM @stage cannot break out via the path."""
        path = "x) UNION SELECT password FROM secrets --"
        stage = ExternalStage(name="EXPORTS", namespace="DB.SCH", path=path)
        sql = sql_compiler(select(text("$1")).select_from(stage))
        expected = (
            "SELECT $1 FROM '@DB.SCH.EXPORTS/x) UNION SELECT password FROM secrets --'"
        )
        assert sql == expected

    def test_snow_4134196_external_stage_path_comment_only_quoting(self, sql_compiler):
        """A path made only of otherwise-safe characters plus ``--`` is still quoted.

        ``foo--bar`` matches the bare-path character allowlist ([A-Za-z0-9_./-])
        on its own, so the explicit ``"--" not in path`` check is the only thing
        stopping it from starting a SQL line comment when emitted bare.
        """
        stage = ExternalStage(name="EXPORTS", namespace="DB.SCH", path="foo--bar")
        sql = sql_compiler(stage)
        assert sql == "'@DB.SCH.EXPORTS/foo--bar'"

    def test_snow_4134196_external_stage_path_single_quote_escaping(self, sql_compiler):
        """A single quote in the path must be doubled inside the quoted literal."""
        stage = ExternalStage(
            name="EXPORTS", namespace="DB.SCH", path="x' OVERWRITE=TRUE --"
        )
        sql = sql_compiler(stage)
        assert sql == "'@DB.SCH.EXPORTS/x'' OVERWRITE=TRUE --'"
        assert "\\'" not in sql  # never backslash-escape a single quote

    def test_snow_4134196_external_stage_path_backslash_escaping(self, sql_compiler):
        r"""A backslash before a quote (\') in the path must be neutralised."""
        stage = ExternalStage(
            name="EXPORTS", namespace="DB.SCH", path="x" + BS + "' --"
        )
        sql = sql_compiler(stage)
        # Python "x\\\\'' --" represents the SQL text: x\\'' --
        assert sql == "'@DB.SCH.EXPORTS/x\\\\'' --'"

    def test_snow_4134196_external_stage_path_with_file_format_quoting(
        self, sql_compiler
    ):
        """An unsafe path is single-quoted even in the inline file_format form."""
        stage = ExternalStage(
            name="EXPORTS", namespace="DB.SCH", path="a b", file_format="my_fmt"
        )
        sql = sql_compiler(stage)
        assert sql == "'@DB.SCH.EXPORTS/a b' (file_format => my_fmt)"

    def test_snow_4134196_external_stage_path_plain_bcr(self, sql_compiler):
        """BCR: an ordinary stage path keeps its historical bare rendering."""
        stage = ExternalStage(
            name="AZURE_STAGE", namespace="ML_POC.PUBLIC", path="testdata/out.parquet"
        )
        sql = sql_compiler(stage)
        assert sql == "@ML_POC.PUBLIC.AZURE_STAGE/testdata/out.parquet"

    def test_snow_4134196_external_stage_from_parent_path_quoting(self, sql_compiler):
        """from_parent_stage sub-paths get the same treatment."""
        root = ExternalStage(name="EXPORTS", namespace="DB.SCH")
        child = ExternalStage.from_parent_stage(
            root, "out PATTERN='.*' FORCE=TRUE PURGE=TRUE --"
        )
        sql = sql_compiler(child)
        assert sql == "'@DB.SCH.EXPORTS/out PATTERN=''.*'' FORCE=TRUE PURGE=TRUE --'"
