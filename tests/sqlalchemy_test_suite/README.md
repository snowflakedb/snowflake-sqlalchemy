# SQLAlchemy Compliance Tests

SQLAlchemy offers tests to test SQLAlchemy dialects work properly. This directory applies these tests
to the Snowflake SQLAlchemy dialect.

**Please be aware that the test suites are not collected in pytest by default** -- the directory is ignored in `pytest.ini`.
This is because importing sqlalchemy pytest plugin will result in Snowflake SQLAlchemy dialect specific tests not
being collected, also the Python parser can not load options correctly from setup.cfg or from pytest plugin steps.
However, running from a separate

To run the SQLAlchemy test suites, please specify the directory of the test suites when running pytest command:

```bash
$cd snowflake-sqlalchemy
$pytest tests/sqlalchemy_test_suite
```
