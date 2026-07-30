#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import urllib.parse

from sqlalchemy.engine.url import make_url

from snowflake.sqlalchemy import URL


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


def test_url_password_with_square_brackets():
    """Square brackets in a password must be percent-encoded (SNOW-828206).

    Otherwise urllib's URL parser treats ``[``/``]`` as an IPv6 host literal and
    raises, breaking otherwise valid RFC 1738 passwords.
    """
    assert (
        URL(account="testaccount", user="admin", password="mypass]")
        == "snowflake://admin:mypass%5D@testaccount/"
    )
    assert (
        URL(account="testaccount", user="admin", password="[mypass")
        == "snowflake://admin:%5Bmypass@testaccount/"
    )
    assert (
        URL(account="testaccount", user="admin", password="a[b]c:@/")
        == "snowflake://admin:a%5Bb%5Dc%3A%40%2F@testaccount/"
    )


def test_url_password_with_query_and_fragment_delimiters():
    """``?`` and ``#`` in a password must be encoded (SNOW-828206).

    They are URL query/fragment delimiters; if left raw they terminate the
    authority component and push the host into the query/fragment.
    """
    assert (
        URL(account="testaccount", user="admin", password="pa?ss")
        == "snowflake://admin:pa%3Fss@testaccount/"
    )
    assert (
        URL(account="testaccount", user="admin", password="pa#ss")
        == "snowflake://admin:pa%23ss@testaccount/"
    )


def test_url_password_with_brackets_roundtrips():
    """The generated URL parses and decodes back to the original password."""
    password = "p[a]s:s@w/o[rd]?x#y"
    generated = URL(account="testaccount", user="admin", password=password)

    # Should not raise (previously ``[``/``]``/``?``/``#`` broke the parser).
    urllib.parse.urlsplit(generated)

    parsed = make_url(generated)
    assert parsed.password == password
