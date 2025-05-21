#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import urllib.parse

from snowflake.sqlalchemy import URL
from sqlalchemy.engine import create_engine
import base64
import pytest


def test_private_key_base64_is_bytes():
    from sqlalchemy.engine.url import make_url
    from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
    dummy_der_bytes = b'\x30\x82\x04\xa3\x02\x01\x00'
    import base64
    b64_pk = base64.b64encode(dummy_der_bytes).decode("ascii")
    uri = f"snowflake://user@account/?private_key={b64_pk}"
    url = make_url(uri)
    dialect = SnowflakeDialect()
    _, opts = dialect.create_connect_args(url)
    assert isinstance(opts["private_key"], bytes), (
        f"private_key should be bytes, got {type(opts['private_key'])}"
    )


def test_private_key_base64_url_typeerror():
    # This is a dummy DER bytes for testing (not a real key)
    dummy_der_bytes = b'\x30\x82\x04\xa3\x02\x01\x00'  # just a few bytes, not a real key
    b64_pk = base64.b64encode(dummy_der_bytes).decode("ascii")
    uri = f"snowflake://user@account/?private_key={b64_pk}"
    # If the dialect patch is NOT present, this would raise TypeError from the connector
    # If the patch IS present, the engine is created and the connection will fail later (not TypeError)
    try:
        engine = create_engine(uri)
        # Try to actually connect (will fail with ProgrammingError due to dummy key, but not TypeError)
        with pytest.raises(Exception) as exc_info:
            with engine.connect() as conn:
                conn.execute("select 1")
    except TypeError as e:
        # This would be the error without the dialect patch
        assert "Expected bytes or RSAPrivateKey" in str(e)
    else:
        # If patch is present, the error should NOT be TypeError about private_key type
        if exc_info.value:
            assert "Expected bytes or RSAPrivateKey" not in str(exc_info.value), (
                "Should not get TypeError for private_key type with dialect patch"
            )


def test_url():
    assert (
        URL(account="testaccount", user="admin", password="test", warehouse="testwh")
        == "snowflake://admin:test@testaccount/?warehouse=testwh"
    )

    assert (
        URL(account="testaccount", user="admin", password="test")
        == "snowflake://admin:test@testaccount/"
    )

    assert (
        URL(
            account="testaccount",
            user="admin",
            password="1-pass 2-pass 3-: 4-@ 5-/ 6-pass",
        )
        == "snowflake://admin:1-pass 2-pass 3-%3A 4-%40 5-%2F 6-pass@testaccount/"
    )

    quoted_password = urllib.parse.quote("kx@% jj5/g")
    assert (
        URL(
            account="testaccount",
            user="admin",
            password=quoted_password,
        )
        == "snowflake://admin:kx%40%25%20jj5%2Fg@testaccount/"
    )

    assert (
        URL(account="testaccount", user="admin", password="test", database="testdb")
        == "snowflake://admin:test@testaccount/testdb"
    )

    assert (
        URL(
            account="testaccount",
            user="admin",
            password="test",
            database="testdb",
            schema="testschema",
        )
        == "snowflake://admin:test@testaccount/testdb/testschema"
    )

    assert (
        URL(
            account="testaccount",
            user="admin",
            password="test",
            database="testdb",
            schema="testschema",
            warehouse="testwh",
        )
        == "snowflake://admin:test@testaccount/testdb/testschema?warehouse"
        "=testwh"
    )

    assert (
        URL(
            host="snowflake.reg.local",
            account="testaccount",
            user="admin",
            password="test",
            database="testdb",
            schema="testschema",
        )
        == "snowflake://admin:test@snowflake.reg.local:443/testdb"
        "/testschema?account=testaccount"
    )

    assert URL(
        user="admin", account="testaccount", password="test", region="eu-central-1"
    ) == ("snowflake://admin:test@testaccount.eu-central-1/")

    assert URL(
        user="admin",
        account="testaccount",
        password="test",
        region="eu-central-1.azure",
    ) == ("snowflake://admin:test@testaccount.eu-central-1.azure/")

    assert URL(
        host="testaccount.eu-central-1.snowflakecomputing.com",
        user="admin",
        account="testaccount",
        password="test",
    ) == (
        "snowflake://admin:test@testaccount.eu-central-1"
        ".snowflakecomputing.com:443/?account=testaccount"
    )

    # empty password should be acceptable in URL utility. The validation will
    # happen in Python connector anyway.
    assert URL(
        host="testaccount.eu-central-1.snowflakecomputing.com",
        user="admin",
        account="testaccount",
    ) == (
        "snowflake://admin:@testaccount.eu-central-1"
        ".snowflakecomputing.com:443/?account=testaccount"
    )

    # authenticator=externalbrowser doesn't require a password.
    assert URL(
        host="testaccount.eu-central-1.snowflakecomputing.com",
        user="admin",
        account="testaccount",
        authenticator="externalbrowser",
    ) == (
        "snowflake://admin:@testaccount.eu-central-1"
        ".snowflakecomputing.com:443/?account=testaccount"
        "&authenticator=externalbrowser"
    )

    # authenticator=oktaurl support
    assert URL(
        user="testuser",
        account="testaccount",
        password="test",
        authenticator="https://testokta.okta.com",
    ) == (
        "snowflake://testuser:test@testaccount"
        "/?authenticator=https%3A%2F%2Ftestokta.okta.com"
    )
