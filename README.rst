Snowflake SQLAlchemy
********************************************************************************

.. image:: https://travis-ci.org/snowflakedb/snowflake-sqlalchemy.svg?branch=master
    :target: https://travis-ci.org/snowflakedb/snowflake-sqlalchemy

.. image:: https://codecov.io/gh/snowflakedb/snowflake-sqlalchemy/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/snowflakedb/snowflake-sqlalchemy

.. image:: https://img.shields.io/pypi/v/snowflake-sqlalchemy.svg
    :target: https://pypi.python.org/pypi/snowflake-sqlalchemy/

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt
    
Snowflake SQLAlchemy runs on the top of the Snowflake Connector for Python as a `dialect <http://docs.sqlalchemy.org/en/latest/dialects/>`_ to bridge a Snowflake database and SQLAlchemy applications.

Prerequisites
================================================================================

Snowflake Connector for Python
----------------------------------------------------------------------

The only requirement for Snowflake SQLAlchemy is the Snowflake Connector for Python; however, the connector does not need to be installed because installing Snowflake SQLAlchemy automatically installs the connector.

Data Analytics and Web Application Frameworks (Optional)
----------------------------------------------------------------------

Snowflake SQLAlchemy can be used with `Pandas <http://pandas.pydata.org/>`_, `Jupyter <http://jupyter.org/>`_ and `Pyramid <http://www.pylonsproject.org/>`_, which provide higher levels of application frameworks for data analytics and web applications. However, building a working environment from scratch is not a trivial task, particularly for novice users. Installing the frameworks requires C compilers and tools, and choosing the right tools and versions is a hurdle that might deter users from using Python applications.

An easier way to build an environment is through `Anaconda <https://www.continuum.io/why-anaconda>`_, which provides a complete, precompiled technology stack for all users, including non-Python experts such as data analysts and students. For Anaconda installation instructions, see the `Anaconda install documentation <https://docs.continuum.io/anaconda/install>`_. The Snowflake SQLAlchemy package can then be installed on top of Anaconda using `pip <https://pypi.python.org/pypi/pip>`_.

Installing Snowflake SQLAlchemy
================================================================================

The Snowflake SQLAlchemy package can be installed from the public PyPI repository using ``pip``:

    .. code-block:: bash

        pip install --upgrade snowflake-sqlalchemy

``pip`` automatically installs all required modules, including the Snowflake Connector for Python.

Verifying Your Installation
================================================================================

#. Create a file (e.g. ``validate.py``) that contains the following Python sample code,
   which connects to Snowflake and displays the Snowflake version:

    .. code-block:: python

        #!/usr/bin/env python
        from sqlalchemy import create_engine

        engine = create_engine(
            'snowflake://{user}:{password}@{account}/'.format(
                user='<your_user_login_name>',
                password='<your_password>',
                account='<your_account_name>',
            )
        )
        try:
            results = engine.execute('select current_version()').fetchone()
            print(results[0])
        finally:
            engine.dispose()

#. Replace *your_user_login_name*, *your_password*, and *your_account_name* with the appropriate values for your Snowflake account and user. For more details, see `Connection Parameters`_ in this topic.

#. Execute the sample code. For example, if you created a file named ``validate.py``:

    .. code-block:: python

        python validate.py

The Snowflake version (e.g. ``1.48.0``) should be displayed.

Parameters and Behavior
================================================================================

As much as possible, Snowflake SQLAlchemy provides compatible functionality for SQLAlchemy applications. For information on using SQLAlchemy, see the `SQLAlchemy documentation <http://docs.sqlalchemy.org/en/latest/>`_.

However, Snowflake SQLAlchemy also provides Snowflake-specific parameters and behavior, which are described in the following sections.

Connection Parameters
-------------------------------------------------------------------------------

Snowflake SQLAlchemy uses the following syntax for the connection string used to connect to Snowflake and initiate a session:

    .. code-block:: python

     'snowflake://<user_login_name>:<password>@<account_name>' 

Where: 

- *user_login_name* is the login name for your Snowflake user.
- *password* is the password for your Snowflake user.
- *account_name* is the name of your Snowflake account. Your account name is included in the URL used to access your account, e.g. ``testaccount`` in ``testaccount.snowflakecomputing.com``.

You can optionally specify the initial database and schema for the Snowflake session by including them at the end of the connection string, separated by ``/``. You can also specify the initial warehouse for the session as a parameter string at the end of the connection string:

    .. code-block:: python

        'snowflake://<user_login_name>:<password>@<account_name>/<database_name>/<schema_name>?warehouse=<warehouse_name>'

.. note::

  After login, the initial database, schema, and warehouse specified in the connection string can always be changed for the session.

The following example calls the ``create_engine`` method with the account name ``testaccount``, user name ``testuser1``, password ``pass``, database ``db``, schema ``public``, and warehouse ``testwh``:

    .. code-block:: python
      
        from sqlalchemy import create_engine
        engine = create_engine(
            'snowflake://testuser1:pass@testaccount/db/public?warehouse=testwh'
        )
 
Other parameters, such as *proxy_host* (proxy host address for Snowflake) and *proxy_port* (proxy server port number), can also be specified as a URI parameter or in ``connect_args`` parameters. For example:

    .. code-block:: python

        from sqlalchemy import create_engine
        engine = create_engine(
            'snowflake://testuser1:pass@testaccount/db/public?warehouse=testwh',
            connect_args={
                'proxy_host': 'localhost',
                'proxy_port': '3128',
            } 
        )

For convenience, you can use the ``snowflake.sqlalchemy.URL`` method to construct the connection string and connect to the database. The following example constructs the same connection string from the previous example:

    .. code-block:: python

        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine

        engine = create_engine(URL(
            account = 'testaccount',
            user = 'testuser1',
            password = 'pass',
            database = 'db',
            schema = 'public',
            warehouse = 'testwh',
            proxy_host = 'localhost',
            proxy_port = '3128'
        ))

Auto-increment Behavior
-------------------------------------------------------------------------------

Auto-incrementing a value requires the ``Sequence`` object. Include the ``Sequence`` object in the primary key column to automatically increment the value as each new record is inserted. For example:

    .. code-block:: python
     
            t = Table('mytable', metadata,
                Column('id', Integer, Sequence('id_seq'), primary_key=True),
                Column(...), ...
            )

Object Name Case Handling
-------------------------------------------------------------------------------

Snowflake stores all case-insensitive object names in uppercase text. In contrast, SQLAlchemy considers all lowercase object names to be case-insensitive. Snowflake SQLAlchemy converts the object name case during schema-level communication, i.e. during table and index reflection. If you use uppercase object names, SQLAlchemy assumes they are case-sensitive and surrounds the names with quotes.

Index Support
-------------------------------------------------------------------------------

Snowflake does not utilize indexes, so neither does Snowflake SQLAlchemy.

Numpy Data Type Support
-------------------------------------------------------------------------------

Snowflake SQLAlchemy supports binding and fetching ``NumPy`` data types. Binding is always supported. To enable fetching ``NumPy`` data types, add ``numpy=True`` to the connection parameters.

The following example shows the round trip of ``numpy.datetime64`` data:

    .. code-block:: python

        import numpy as np
        import pandas as pd
        engine = create_engine(URL(
            account = 'testaccount',
            user = 'testuser1',
            password = 'pass',
            database = 'db',
            schema = 'public',
            warehouse = 'testwh',
            numpy=True,
        ))
    
        specific_date = np.datetime64('2016-03-04T12:03:05.123456789Z')
        engine.execute(
            "CREATE OR REPLACE TABLE ts_tbl(c1 TIMESTAMP_NTZ)")
        engine.execute(
            "INSERT INTO ts_tbl(c1) values(%s)", (specific_date,)
        )
        df = pd.read_sql_query("SELECT * FROM ts_tbl", engine)
        assert df.c1.values[0] == specific_date

The following ``NumPy`` data types are supported:

- numpy.int64
- numpy.float64
- numpy.datatime64

VARIANT, ARRAY and OBJECT Support
-------------------------------------------------------------------------------

Snowflake SQLAlchemy supports fetching ``VARIANT``, ``ARRAY`` and ``OBJECT`` data types. All types are converted into ``str`` in Python so that you can convert them to native data types using ``json.loads``.

This example shows how to create a table including ``VARIANT``, ``ARRAY``, and ``OBJECT`` data type columns.

    .. code-block:: python

        from snowflake.sqlalchemy import (VARIANT, ARRAY, OBJECT)
        ...
        t = Table('my_semi_strucutred_datatype_table', metadata,
            Column('va', VARIANT),
            Column('ob', OBJECT),
            Column('ar', ARRAY))
        metdata.create_all(engine)

In order to retrieve ``VARIANT``, ``ARRAY``, and ``OBJECT`` data type columns and convert them to the native Python data types, fetch data and call the ``json.loads`` method as follows:

    .. code-block:: python

        import json
        conn = engine.connect()
        results = conn.execute(select([t])
        row = results.fetchone()
        data_variant = json.loads(row[0])
        data_object  = json.loads(row[1])
        data_array   = json.loads(row[2])

CLUSTER BY Support
-------------------------------------------------------------------------------

Snowflake SQLAchemy supports the ``CLUSTER BY`` parameter for tables. For information about the parameter, see :doc:`/sql-reference/sql/create-table`.

This example shows how to create a table with two columns, ``id`` and ``name``, as the clustering keys:

    .. code-block:: python

        t = Table('myuser', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String),
            snowflake_clusterby=['id', 'name'], ...
        )
        metadata.create_all(engine)
