Building and Testing Snowflake SQLAlchemy
********************************************************************************

Building
================================================================================

Install Python 3.5.0 or higher. Clone the Snowflake SQLAlchemy repository, then run the following command to create a wheel package:

    .. code-block:: bash

        git clone git@github.com:snowflakedb/snowflake-sqlalchemy.git
        cd snowflake-sqlalchemy
        pyvenv /tmp/test_snowflake_sqlalchemy
        source /tmp/test_snowflake_sqlalchemy/bin/activate
        pip install -U pip setuptools wheel
        python setup.py bdist_wheel

Find the ``snowflake-sqlalchemy*.whl`` package in the ``./dist`` directory.


Testing
================================================================================

Create a virtualenv, with ``parameters.py`` in a test directory.

    .. code-block:: bash

        pyvenv /tmp/test_snowflake_sqlalchemy
        source /tmp/test_snowflake_sqlalchemy/bin/activate
        pip install Cython
        pip install pytest numpy pandas
        pip install dist/snowflake_sqlalchemy*.whl
        vim test/parameters.py

In the ``parameters.py`` file, include the connection information in a Python dictionary.
NB: if you do not want to run your tests against an actual Snowflake installation,
use the MockEngine (see below).

    .. code-block:: python

        CONNECTION_PARAMETERS = {
            'account':  'testaccount',
            'user':     'user1',
            'password': 'testpasswd',
            'schema':   'testschema',
            'database': 'testdb',
        }

Run the test:

    .. code-block:: bash

        py.test test

Testing against a Mock engine or a real Snowflake engine
================================================================================
You can choose to run the tests either against a real Snowflake engine, or a mock
engine (as provided by SQLAlchemy), or both.
In case you choose to test against the MockEngine, your test should validate that
the SQL produced by the compiler is the Snowflake-compatible SQL you expected.
However, this SQL is not verified or executed by a realistic engine; so you have to
take care yourself that what you EXPECT to be correct SQL IS in fact correct SQL as
Snowflake recognizes it.
Accordingly, SQL executed by a MockEngine will neither produce results nor fail with
an error. Therefore, some test logic (e.g. most of the tests in test_core.py) which
validate actual test results cannot be executed against a Mock engine, and therefore
are skipped if a Mock engine is configured.
In case you run the tests against a real Snowflake engine, you have to take care that
you have access to a running Snowflake installation - and take into account that
any intermediate results of your test are cleaned up after the execution.

Usage of the Mock Engine or Snowflake Engine is configured in the connection_type
fixture in conftest.py.
