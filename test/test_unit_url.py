from snowflake.sqlalchemy import URL


def test_url():
    assert URL(account='testaccount', user='admin',
               password='test', warehouse='testwh') == \
           "snowflake://admin:test@testaccount/?warehouse=testwh"

    assert URL(account='testaccount', user='admin',
               password='test') == "snowflake://admin:test@testaccount/"

    assert URL(account='testaccount', user='admin',
               password='test', database='testdb') == \
           "snowflake://admin:test@testaccount/testdb"

    assert URL(account='testaccount', user='admin',
               password='test', database='testdb', schema='testschema') == \
           "snowflake://admin:test@testaccount/testdb/testschema"

    assert URL(account='testaccount', user='admin',
               password='test', database='testdb', schema='testschema',
               warehouse='testwh') == \
           "snowflake://admin:test@testaccount/testdb/testschema?warehouse" \
           "=testwh"

    assert URL(host='snowflake.reg.local', account='testaccount', user='admin',
               password='test', database='testdb', schema='testschema') == \
           "snowflake://admin:test@snowflake.reg.local:443/testdb" \
           "/testschema?account=testaccount"
