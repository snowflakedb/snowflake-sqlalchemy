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
        vim tests/parameters.py

In the ``parameters.py`` file, include the connection information in a Python dictionary.

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


DECFLOAT Precision
================================================================================

Snowflake's DECFLOAT type supports up to 38 significant digits. However, Python's
default decimal context precision is 28 digits, which can truncate values when
reading from the database.

To preserve full 38-digit precision, add ``enable_decfloat=True`` to the connection URL:

    .. code-block:: python

        from decimal import Decimal
        from sqlalchemy import create_engine, Column, Integer, MetaData, Table, select
        from snowflake.sqlalchemy import DECFLOAT

        # Use enable_decfloat=True in the connection URL for full precision
        engine = create_engine(
            'snowflake://user:password@account/database/schema?enable_decfloat=True'
        )
        metadata = MetaData()

        prices = Table(
            'prices', metadata,
            Column('id', Integer, primary_key=True),
            Column('value', DECFLOAT()),
        )
        metadata.create_all(engine)

        with engine.connect() as conn:
            # Insert a value with 38 significant digits
            conn.execute(prices.insert().values(
                id=1,
                value=Decimal('12345678901234567890123456789.123456789')
            ))
            conn.commit()

            # Query returns full precision with enable_decfloat=True
            result = conn.execute(select(prices)).fetchone()
            print(result[1])  # 12345678901234567890123456789.123456789

Alternatively, you can set Python's decimal context manually:

    .. code-block:: python

        import decimal
        decimal.getcontext().prec = 38
