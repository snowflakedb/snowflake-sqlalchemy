#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""PUT/GET file-transfer coverage on the sqlalchemy.testing framework.

SQL-string PUT/GET are executed through the cursor, so they ride the async
adapter unchanged: this test runs against both snowflake:// and
snowflake+aiosnowflake:// (driver chosen by --dburi) with no async-specific
code. It exercises the connector's file-transfer path (in 5.x, the Rust core)
end to end: create a stage, PUT a local file, LIST it, GET it back, and verify
the round-tripped bytes.
"""

import os
import tempfile

import pytest
from sqlalchemy.testing import fixtures


@pytest.mark.skipif(
    os.getenv("SNOWFLAKE_GCP") is not None,
    reason="PUT and GET are not supported for GCP",
)
@pytest.mark.skipif(
    os.name == "nt",
    reason="PUT/GET file:// path handling differs on Windows",
)
class PutGetTest(fixtures.TestBase):
    __backend__ = True

    def test_put_get_roundtrip(self, connection):
        stage = "fw_put_get_stage"
        filename = "fw_putget.csv"
        payload = b"col1,col2\n1,alpha\n2,beta\n"

        connection.exec_driver_sql(f"CREATE OR REPLACE TEMPORARY STAGE {stage}")

        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, filename)
            with open(src, "wb") as fh:
                fh.write(payload)

            # PUT: upload the local file to the stage (no auto-compress so the
            # staged filename stays predictable for the GET below).
            put_rows = connection.exec_driver_sql(
                f"PUT file://{src} @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            ).fetchall()
            assert len(put_rows) == 1
            # PUT result columns end with (..., status, message); status=UPLOADED
            assert any(str(v).upper() == "UPLOADED" for v in put_rows[0]), put_rows[0]

            listed = connection.exec_driver_sql(f"LIST @{stage}").fetchall()
            assert any(filename in str(row[0]) for row in listed), listed

            # GET: download it back to a fresh directory and verify the bytes.
            dest = os.path.join(tmp, "download")
            os.makedirs(dest, exist_ok=True)
            get_rows = connection.exec_driver_sql(
                f"GET @{stage}/{filename} file://{dest}"
            ).fetchall()
            assert len(get_rows) == 1
            assert any(str(v).upper() == "DOWNLOADED" for v in get_rows[0]), get_rows[0]

            with open(os.path.join(dest, filename), "rb") as fh:
                assert fh.read() == payload
