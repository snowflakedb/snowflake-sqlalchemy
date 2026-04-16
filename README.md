# Snowflake SQLAlchemy

[![Build and Test](https://github.com/snowflakedb/snowflake-sqlalchemy/actions/workflows/build_test.yml/badge.svg)](https://github.com/snowflakedb/snowflake-sqlalchemy/actions/workflows/build_test.yml)
[![codecov](https://codecov.io/gh/snowflakedb/snowflake-sqlalchemy/branch/main/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-sqlalchemy)
[![PyPi](https://img.shields.io/pypi/v/snowflake-sqlalchemy.svg)](https://pypi.python.org/pypi/snowflake-sqlalchemy/)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Codestyle Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Snowflake SQLAlchemy runs on the top of the Snowflake Connector for Python as a [dialect](http://docs.sqlalchemy.org/en/latest/dialects/) to bridge a Snowflake database and SQLAlchemy applications.

Table of contents:
<!-- TOC -->
* [Snowflake SQLAlchemy](#snowflake-sqlalchemy)
  * [Prerequisites](#prerequisites)
    * [Snowflake Connector for Python](#snowflake-connector-for-python)
    * [Data Analytics and Web Application Frameworks (Optional)](#data-analytics-and-web-application-frameworks-optional)
  * [Installing Snowflake SQLAlchemy](#installing-snowflake-sqlalchemy)
  * [Verifying Your Installation](#verifying-your-installation)
  * [Parameters and Behavior](#parameters-and-behavior)
    * [Connection Parameters](#connection-parameters)
      * [Escaping Special Characters such as `%, @` signs in Passwords](#escaping-special-characters-such-as---signs-in-passwords)
      * [Using a proxy server](#using-a-proxy-server)
      * [Using session parameters](#using-session-parameters)
    * [Opening and Closing Connection](#opening-and-closing-connection)
    * [Auto-increment Behavior](#auto-increment-behavior)
    * [Object Name Case Handling](#object-name-case-handling)
    * [Index Support](#index-support)
      * [Single Column Index](#single-column-index)
      * [Multi-Column Index](#multi-column-index)
    * [Numpy Data Type Support](#numpy-data-type-support)
    * [DECFLOAT Data Type Support](#decfloat-data-type-support)
      * [DECFLOAT Precision](#decfloat-precision)
    * [VECTOR Data Type Support](#vector-data-type-support)
    * [Cache Column Metadata](#cache-column-metadata)
    * [VARIANT, ARRAY and OBJECT Support](#variant-array-and-object-support)
    * [Structured Data Types Support](#structured-data-types-support)
      * [MAP](#map)
      * [OBJECT](#object)
      * [ARRAY](#array)
    * [CLUSTER BY Support](#cluster-by-support)
    * [Alembic Support](#alembic-support)
    * [Key Pair Authentication Support](#key-pair-authentication-support)
    * [Merge Command Support](#merge-command-support)
    * [CopyIntoStorage Support](#copyintostorage-support)
    * [Iceberg Table with Snowflake Catalog support](#iceberg-table-with-snowflake-catalog-support)
    * [Hybrid Table support](#hybrid-table-support)
    * [Dynamic Tables support](#dynamic-tables-support)
    * [Notes](#notes)
  * [Verifying Package Signatures](#verifying-package-signatures)
  * [Support](#support)
  * [Known Limitations](#known-limitations)
    * [Identity columns as primary keys](#identity-columns-as-primary-keys)
    * [Case-sensitive identifiers](#case-sensitive-identifiers)
<!-- TOC -->

## Prerequisites

### Snowflake Connector for Python

The only requirement for Snowflake SQLAlchemy is the Snowflake Connector for Python; however, the connector does not need to be installed because installing Snowflake SQLAlchemy automatically installs the connector.

### Data Analytics and Web Application Frameworks (Optional)

Snowflake SQLAlchemy can be used with [Pandas](http://pandas.pydata.org/), [Jupyter](http://jupyter.org/) and [Pyramid](http://www.pylonsproject.org/), which provide higher levels of application frameworks for data analytics and web applications. However, building a working environment from scratch is not a trivial task, particularly for novice users. Installing the frameworks requires C compilers and tools, and choosing the right tools and versions is a hurdle that might deter users from using Python applications.

An easier way to build an environment is through [Anaconda](https://www.continuum.io/why-anaconda), which provides a complete, precompiled technology stack for all users, including non-Python experts such as data analysts and students. For Anaconda installation instructions, see the [Anaconda install documentation](https://docs.continuum.io/anaconda/install). The Snowflake SQLAlchemy package can then be installed on top of Anaconda using [pip](https://pypi.python.org/pypi/pip).

## Installing Snowflake SQLAlchemy

The Snowflake SQLAlchemy package can be installed from the public PyPI repository using `pip`:

```shell
pip install --upgrade snowflake-sqlalchemy
```

`pip` automatically installs all required modules, including the Snowflake Connector for Python.

## Verifying Your Installation

1. Create a file (e.g. `validate.py`) that contains the following Python sample code,
   which connects to Snowflake and displays the Snowflake version:

    ```python
    from sqlalchemy import create_engine

    engine = create_engine(
        'snowflake://{user}:{password}@{account}/'.format(
            user='<your_user_login_name>',
            password='<your_password>',
            account='<your_account_name>',
        )
    )
    try:
        connection = engine.connect()
        results = connection.execute('select current_version()').fetchone()
        print(results[0])
    finally:
        connection.close()
        engine.dispose()
    ```

2. Replace `<your_user_login_name>`, `<your_password>`, and `<your_account_name>` with the appropriate values for your Snowflake account and user.

    For more details, see [Connection Parameters](#connection-parameters).

3. Execute the sample code. For example, if you created a file named `validate.py`:

    ```shell
    python validate.py
    ```

    The Snowflake version (e.g. `1.48.0`) should be displayed.

## Parameters and Behavior

As much as possible, Snowflake SQLAlchemy provides compatible functionality for SQLAlchemy applications. For information on using SQLAlchemy, see the [SQLAlchemy documentation](http://docs.sqlalchemy.org/en/latest/).

However, Snowflake SQLAlchemy also provides Snowflake-specific parameters and behavior, which are described in the following sections.

### Connection Parameters

Snowflake SQLAlchemy uses the following syntax for the connection string used to connect to Snowflake and initiate a session:

```python
'snowflake://<user_login_name>:<password>@<account_name>'
```

Where:

- `<user_login_name>` is the login name for your Snowflake user.
- `<password>` is the password for your Snowflake user.
- `<account_name>` is the name of your Snowflake account.

Include the region in the `<account_name>` if applicable, more info is available [here](https://docs.snowflake.com/en/user-guide/connecting.html#your-snowflake-account-name).

You can optionally specify the initial database and schema for the Snowflake session by including them at the end of the connection string, separated by `/`. You can also specify the initial warehouse and role for the session as a parameter string at the end of the connection string:

```python
'snowflake://<user_login_name>:<password>@<account_name>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'
```

#### Escaping Special Characters such as `%, @` signs in Passwords

As pointed out in [SQLAlchemy](https://docs.sqlalchemy.org/en/14/core/engines.html#escaping-special-characters-such-as-signs-in-passwords), URLs
containing special characters need to be URL encoded to be parsed correctly. This includes the `%, @` signs. Unescaped password containing special
characters could lead to authentication failure.

The encoding for the password can be generated using `urllib.parse`:

```python
import urllib.parse
urllib.parse.quote("kx@% jj5/g")
'kx%40%25%20jj5/g'
```

**Note**: `urllib.parse.quote_plus` may also be used if there is no space in the string, as `urllib.parse.quote_plus` will replace space with `+`.

To create an engine with the proper encodings, either manually constructing the url string by formatting
or taking advantage of the `snowflake.sqlalchemy.URL` helper method:

```python
import urllib.parse
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

quoted_password = urllib.parse.quote("kx@% jj5/g")

# 1. manually constructing an url string
url = f'snowflake://testuser1:{quoted_password}@abc123/testdb/public?warehouse=testwh&role=myrole'
engine = create_engine(url)

# 2. using the snowflake.sqlalchemy.URL helper method
engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = quoted_password,
    database = 'testdb',
    schema = 'public',
    warehouse = 'testwh',
    role='myrole',
))
```

**Note**:
After login, the initial database, schema, warehouse and role specified in the connection string can always be changed for the session.

The following example calls the `create_engine` method with the user name `testuser1`, password `0123456`, account name `abc123`, database `testdb`, schema `public`, warehouse `testwh`, and role `myrole`:

```python
from sqlalchemy import create_engine
engine = create_engine(
    'snowflake://testuser1:0123456@abc123/testdb/public?warehouse=testwh&role=myrole'
)
```

Other parameters, such as `timezone`, can also be specified as a URI parameter or in `connect_args` parameters. For example:

```python
from sqlalchemy import create_engine
engine = create_engine(
    'snowflake://testuser1:0123456@abc123/testdb/public?warehouse=testwh&role=myrole',
    connect_args={
        'timezone': 'America/Los_Angeles',
    }
)
```

For convenience, you can use the `snowflake.sqlalchemy.URL` method to construct the connection string and connect to the database. The following example constructs the same connection string from the previous example:

```python
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = '0123456',
    database = 'testdb',
    schema = 'public',
    warehouse = 'testwh',
    role='myrole',
    timezone = 'America/Los_Angeles',
))
```

#### Using a proxy server

Use the supported environment variables, `HTTPS_PROXY`, `HTTP_PROXY` and `NO_PROXY` to configure a proxy server.

#### Using session parameters

Snowflake [session parameters](https://docs.snowflake.com/en/sql-reference/parameters#session-parameters) (such as [`QUERY_TAG`](https://docs.snowflake.com/en/sql-reference/parameters#query-tag)) cannot be set directly through the `URL` helper.
Instead, pass them via the `connect_args` parameter of `create_engine`, using the `session_parameters` dict — the same way you would [through the Python connector](https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-session-parameters):

```python
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

engine = create_engine(
    URL(
        # CONNECTION_PARAMETERS
    ),
    connect_args={
        "session_parameters": {
            "QUERY_TAG": "SOME_QUERY_TAGS",
        }
    },
)
```

Session parameters set this way apply to all queries executed within the session.
To change a session parameter for specific queries mid-session, use `ALTER SESSION`:

```python
from sqlalchemy import text

with engine.connect() as conn:
    conn.execute(text("ALTER SESSION SET QUERY_TAG = 'batch_job_1'"))
    conn.execute(text("..."))  # Uses 'batch_job_1'

    conn.execute(text("ALTER SESSION SET QUERY_TAG = 'batch_job_2'"))
    conn.execute(text("..."))  # Uses 'batch_job_2'

    conn.execute(text("ALTER SESSION UNSET QUERY_TAG"))
    conn.execute(text("..."))  # No tag
```

### Opening and Closing Connection

Open a connection by executing `engine.connect()`; avoid using `engine.execute()`. Make certain to close the connection by executing `connection.close()` before
`engine.dispose()`; otherwise, the Python Garbage collector removes the resources required to communicate with Snowflake, preventing the Python connector from closing the session properly.

```python
# Avoid this.
engine = create_engine(...)
engine.execute(<SQL>)
engine.dispose()

# Better.
engine = create_engine(...)
connection = engine.connect()
try:
  connection.execute(text(<SQL>))
finally:
    connection.close()
    engine.dispose()

# Best
try:
    with engine.connect() as connection:
        connection.execute(text(<SQL>))
        # or
        connection.exec_driver_sql(<SQL>)
finally:
    engine.dispose()
```

### Auto-increment Behavior

Auto-incrementing a value requires the `Sequence` object. Include the `Sequence` object in the primary key column to automatically increment the value as each new record is inserted. For example:

```python
t = Table('mytable', metadata,
    Column('id', Integer, Sequence('id_seq'), primary_key=True),
    Column(...), ...
)
```

### Object Name Case Handling

Snowflake stores all case-insensitive object names in uppercase text. In contrast, SQLAlchemy considers all lowercase object names to be case-insensitive. Snowflake SQLAlchemy converts the object name case during schema-level communication, i.e. during table and index reflection. If you use uppercase object names, SQLAlchemy assumes they are case-sensitive and encloses the names with quotes. This behavior will cause mismatches against data dictionary data received from Snowflake, so unless identifier names have been truly created as case sensitive using quotes, e.g., `"TestDb"`, all lowercase names should be used on the SQLAlchemy side.

### Index Support

Indexes are supported only for Hybrid Tables in Snowflake SQLAlchemy. For more details on limitations and use cases, refer to the [Create Index documentation](https://docs.snowflake.com/en/sql-reference/constraints-indexes.html). You can create an index using the following methods:

#### Single Column Index

You can create a single column index by setting the `index=True` parameter on the column or by explicitly defining an `Index` object.

```python
hybrid_test_table_1 = HybridTable(
  "table_name",
  metadata,
  Column("column1", Integer, primary_key=True),
  Column("column2", String, index=True),
  Index("index_1", "column1", "column2")
)

metadata.create_all(engine_testaccount)
```

#### Multi-Column Index

For multi-column indexes, you define the `Index` object specifying the columns that should be indexed.

```python
hybrid_test_table_1 = HybridTable(
  "table_name",
  metadata,
  Column("column1", Integer, primary_key=True),
  Column("column2", String),
  Index("index_1", "column1", "column2")
)

metadata.create_all(engine_testaccount)
```

### Numpy Data Type Support

Snowflake SQLAlchemy supports binding and fetching `NumPy` data types. Binding is always supported. To enable fetching `NumPy` data types, add `numpy=True` to the connection parameters.

The following example shows the round trip of `numpy.datetime64` data:

```python
import numpy as np
import pandas as pd
engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = 'pass',
    database = 'db',
    schema = 'public',
    warehouse = 'testwh',
    role='myrole',
    numpy=True,
))

specific_date = np.datetime64('2016-03-04T12:03:05.123456789Z')

with engine.connect() as connection:
    connection.exec_driver_sql(
        "CREATE OR REPLACE TABLE ts_tbl(c1 TIMESTAMP_NTZ)")
    connection.exec_driver_sql(
        "INSERT INTO ts_tbl(c1) values(%s)", (specific_date,)
    )
    df = pd.read_sql_query("SELECT * FROM ts_tbl", connection)
    assert df.c1.values[0] == specific_date
```

The following `NumPy` data types are supported:

- numpy.int64
- numpy.float64
- numpy.datatime64

### DECFLOAT Data Type Support

Snowflake SQLAlchemy supports the `DECFLOAT` data type, which provides decimal floating-point with up to 38 significant digits. For more information, see the [Snowflake DECFLOAT documentation](https://docs.snowflake.com/en/sql-reference/data-types-numeric#decfloat).

```python
from sqlalchemy import Column, Integer, MetaData, Table
from snowflake.sqlalchemy import DECFLOAT

metadata = MetaData()
t = Table('my_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('value', DECFLOAT()),
)
metadata.create_all(engine)
```

#### DECFLOAT Precision

The Snowflake Python connector uses Python's `decimal` module context when converting `DECFLOAT` values to Python `Decimal` objects. Python's default decimal context precision is 28 digits, which can truncate `DECFLOAT` values that use up to 38 digits.

To preserve full 38-digit precision, add `enable_decfloat=True` to the connection URL:

```python
from sqlalchemy import create_engine

engine = create_engine(
    'snowflake://testuser1:0123456@abc123/testdb/public?warehouse=testwh&enable_decfloat=True'
)
```

Or using the `snowflake.sqlalchemy.URL` helper:

```python
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = '0123456',
    database = 'testdb',
    schema = 'public',
    warehouse = 'testwh',
    enable_decfloat = True,
))
```

**Note**: `DECFLOAT` does not support special values (`inf`, `-inf`, `NaN`) unlike `FLOAT`.

**Why is `enable_decfloat` not enabled by default?** Enabling it sets `decimal.getcontext().prec = 38`, which modifies Python's thread-local decimal context and affects all `Decimal` operations in that thread, not just database queries. To avoid unexpected side effects on application code, the dialect emits a warning when `DECFLOAT` values are retrieved without full precision enabled, guiding users to opt-in explicitly.

### VECTOR Data Type Support

Snowflake SQLAlchemy supports the `VECTOR` data type with varying element type and dimension.
For more information, see the [Snowflake documentation](https://docs.snowflake.com/en/sql-reference/data-types-vector).

```python
from sqlalchemy import Column, Integer, Float, MetaData, Table
from snowflake.sqlalchemy import VECTOR

metadata = MetaData()
t = Table('my_table', metadata,
    Column('id', Integer, primary_key=True),
    Column('int_vec', VECTOR(Integer, 20)),
    Column('float_vec', VECTOR(Float, 40)),
)
metadata.create_all(engine)
```

### Timestamp and Timezone Support

Snowflake SQLAlchemy provides three Snowflake-specific timestamp types that map directly to their Snowflake counterparts:

```python
from sqlalchemy import Column, Integer, MetaData, Table, create_engine
from snowflake.sqlalchemy import TIMESTAMP_NTZ, TIMESTAMP_TZ, TIMESTAMP_LTZ

engine = create_engine(...)
metadata = MetaData()
t = Table('events', metadata,
    Column('id', Integer, primary_key=True),
    Column('created_at', TIMESTAMP_NTZ()),   # TIMESTAMP WITHOUT TIME ZONE
    Column('scheduled_at', TIMESTAMP_TZ()),  # TIMESTAMP WITH TIME ZONE
    Column('logged_at', TIMESTAMP_LTZ()),    # TIMESTAMP WITH LOCAL TIME ZONE
)
metadata.create_all(engine)
```

SQLAlchemy's generic `DateTime` and `TIMESTAMP` types also support timezone-aware columns via the `timezone` parameter. When `timezone=True` is set, the dialect emits `TIMESTAMP_TZ` instead of the default `TIMESTAMP_NTZ`:

```python
from sqlalchemy import Column, DateTime, Integer, MetaData, Table, create_engine
from sqlalchemy.types import TIMESTAMP

engine = create_engine(...)
metadata = MetaData()
t = Table('events', metadata,
    Column('id', Integer, primary_key=True),
    Column('naive_ts', DateTime()),                 # produces TIMESTAMP_NTZ
    Column('aware_ts', DateTime(timezone=True)),    # produces TIMESTAMP_TZ
    Column('naive_ts2', TIMESTAMP()),               # produces TIMESTAMP_NTZ
    Column('aware_ts2', TIMESTAMP(timezone=True)),  # produces TIMESTAMP_TZ
)
metadata.create_all(engine)
```

This also applies when using pandas `to_sql()` with timezone-aware datetime columns, which infers `DateTime(timezone=True)` automatically (see [#199](https://github.com/snowflakedb/snowflake-sqlalchemy/issues/199)).

**Note on `Time` and timezones:** SQLAlchemy's `Time` type accepts a `timezone` parameter, but [Snowflake's TIME data type does not support time zones](https://docs.snowflake.com/en/sql-reference/data-types-datetime#time). Using `Time(timezone=True)` will compile to plain `TIME` and the `timezone` flag will have no effect. If you need to store time data with time-zone information, use a timestamp type such as `TIMESTAMP_TZ` or `DateTime(timezone=True)` instead.

### Cache Column Metadata

SQLAlchemy provides [the runtime inspection API](http://docs.sqlalchemy.org/en/latest/core/inspection.html) to get the runtime information about the various objects. One common use case is retrieving all tables and their column metadata in a schema to construct a schema catalog. For example, [alembic](http://alembic.zzzcomputing.com/) manages database schema migrations on top of SQLAlchemy. A typical flow (SQLAlchemy 1.4) is:

```python
inspector = inspect(engine)
schema = inspector.default_schema_name
for table_name in inspector.get_table_names(schema):
    column_metadata = inspector.get_columns(table_name, schema)
    primary_keys = inspector.get_pk_constraint(table_name, schema)
    foreign_keys = inspector.get_foreign_keys(table_name, schema)
    ...
```

In this flow, running a separate query per table can be slow for large schemas. Snowflake SQLAlchemy optimises this with schema-wide cached queries and, where appropriate, fast per-table queries.

#### Single-Table vs Multi-Table Reflection Performance

**SQLAlchemy 2.x (automatic)**

SQLAlchemy 2.x distinguishes bulk reflection from single-table inspection at the framework level:

- **`MetaData.reflect()` / `Table(..., autoload_with=engine)`** — calls `get_multi_columns`, `get_multi_pk_constraint`, `get_multi_foreign_keys`, and `get_multi_unique_constraints`. Each issues one schema-wide `SHOW` or `information_schema` query and caches the result for all tables in the schema.
- **`inspector.get_columns(table_name)`** — issues a single `DESC TABLE` query directly against that table. This is fast and correct for all table types including temporary tables.

No configuration is needed; the routing is handled automatically by the SA 2.x dispatch layer.

**Note on reflected type representations:** Because `inspector.get_columns()` uses `DESC TABLE`, reflected types always include Snowflake's resolved default sizes (e.g. `BINARY(8388608)` instead of `BINARY`, `VARCHAR(16777216)` instead of `VARCHAR`). The type *objects* are functionally identical; only `str()` output differs. Use `isinstance()` checks rather than string comparison for type introspection.

```python
from sqlalchemy import MetaData, inspect, create_engine

engine = create_engine('snowflake://...')

# SA 2.x: one schema-wide query per metadata type, all tables cached at once
metadata = MetaData()
metadata.reflect(bind=engine, schema='public')

# SA 2.x: direct DESC TABLE, no schema-wide query issued
inspector = inspect(engine)
columns = inspector.get_columns('my_table', schema='public')
```

**SQLAlchemy 1.4 and per-table optimisation (opt-in)**

In SQLAlchemy 1.4, `MetaData.reflect()` calls `get_columns`, `get_pk_constraint`, etc. per table. For schemas with many tables this generates many round-trips. Add `cache_column_metadata=True` to the connection URL to opt in to per-table `SHOW … IN TABLE` and `DESC TABLE` queries for `get_pk_constraint`, `get_unique_constraints`, `get_foreign_keys`, `get_indexes`, and `get_columns`. Each query targets only the requested table and is not cached, so it always reflects the current state.

```python
engine = create_engine(URL(
    account = 'abc123',
    user = 'testuser1',
    password = 'pass',
    database = 'db',
    schema = 'public',
    warehouse = 'testwh',
    role='myrole',
    cache_column_metadata=True,
))
```

This flag also enables the per-table path for the singular `Inspector` methods (`get_pk_constraint`, `get_unique_constraints`, `get_foreign_keys`, `get_indexes`) under SQLAlchemy 2.x.

**Performance Implications**

For schemas with many tables (100+), schema-wide queries issued once during `MetaData.reflect()` are far more efficient than per-table queries in a loop:

- Schema-wide `SHOW PRIMARY KEYS IN SCHEMA` (all tables): < 1 second
- Per-table loop over 1 000 tables: 1 000+ round-trips

For single-table inspection via `Inspector`, per-table queries (`DESC TABLE`, `SHOW … IN TABLE`) are faster than fetching the entire schema.

**Best Practices**

1. **For bulk reflection**: use `metadata.reflect()` — schema-wide queries are issued once and cached.
2. **For single-table inspection**: use `inspector.get_columns()` / `inspector.get_pk_constraint()` etc. — per-table queries are used automatically (SA 2.x) or with `cache_column_metadata=True` (SA 1.4).
3. **For very large schemas**: reflect only the tables you need:

```python
metadata.reflect(bind=engine, schema='public', only=['table1', 'table2'])
```

### VARIANT, ARRAY and OBJECT Support

Snowflake SQLAlchemy supports fetching `VARIANT`, `ARRAY` and `OBJECT` data types. All types are converted into `str` in Python so that you can convert them to native data types using `json.loads`.

This example shows how to create a table including `VARIANT`, `ARRAY`, and `OBJECT` data type columns.

```python
from snowflake.sqlalchemy import (VARIANT, ARRAY, OBJECT)

t = Table('my_semi_strucutred_datatype_table', metadata,
    Column('va', VARIANT),
    Column('ob', OBJECT),
    Column('ar', ARRAY))
metdata.create_all(engine)
```

In order to retrieve `VARIANT`, `ARRAY`, and `OBJECT` data type columns and convert them to the native Python data types, fetch data and call the `json.loads` method as follows:

```python
import json
connection = engine.connect()
results = connection.execute(select([t])
row = results.fetchone()
data_variant = json.loads(row[0])
data_object  = json.loads(row[1])
data_array   = json.loads(row[2])
```

### Structured Data Types Support

This module defines custom SQLAlchemy types for Snowflake structured data, specifically for **Iceberg tables**.
The types —**MAP**, **OBJECT**, and **ARRAY**— allow you to store complex data structures in your SQLAlchemy models.
For detailed information, refer to the Snowflake [Structured data types](https://docs.snowflake.com/en/sql-reference/data-types-structured) documentation.

---

#### MAP

The `MAP` type represents a collection of key-value pairs, where each key and value can have different types.

- **Key Type**: The type of the keys (e.g., `TEXT`, `NUMBER`).
- **Value Type**: The type of the values (e.g., `TEXT`, `NUMBER`).
- **Not Null**: Whether `NULL` values are allowed (default is `False`).

*Example Usage*

```python
IcebergTable(
    table_name,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("map_col", MAP(NUMBER(10, 0), TEXT(16777216))),
    external_volume="external_volume",
    base_location="base_location",
)
```

#### OBJECT

The `OBJECT` type represents a semi-structured object with named fields. Each field can have a specific type, and you can also specify whether each field is nullable.

- **Items Types**: A dictionary of field names and their types. The type can optionally include a nullable flag (`True` for not nullable, `False` for nullable, default is `False`).

*Example Usage*

```python
IcebergTable(
    table_name,
    metadata,
    Column("id", Integer, primary_key=True),
    Column(
        "object_col",
        OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
        OBJECT(key1=TEXT(16777216), key2=NUMBER(10, 0)), # Without nullable flag
    ),
    external_volume="external_volume",
    base_location="base_location",
)
```

#### ARRAY

The `ARRAY` type represents an ordered list of values, where each element has the same type. The type of the elements is defined when creating the array.

- **Value Type**: The type of the elements in the array (e.g., `TEXT`, `NUMBER`).
- **Not Null**: Whether `NULL` values are allowed (default is `False`).

*Example Usage*

```python
IcebergTable(
    table_name,
    metadata,
    Column("id", Integer, primary_key=True),
    Column("array_col", ARRAY(TEXT(16777216))),
    external_volume="external_volume",
    base_location="base_location",
)
```


### CLUSTER BY Support

Snowflake SQLAchemy supports the `CLUSTER BY` parameter for tables. For information about the parameter, see :doc:`/sql-reference/sql/create-table`.

This example shows how to create a table with two columns, `id` and `name`, as the clustering keys:

```python
t = Table('myuser', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    snowflake_clusterby=['id', 'name', text('id > 5')], ...
)
metadata.create_all(engine)
```

### Alembic Support

[Alembic](http://alembic.zzzcomputing.com) is a database migration tool on top of `SQLAlchemy`. Snowflake SQLAlchemy works by adding the following code to `alembic/env.py` so that Alembic can recognize Snowflake SQLAlchemy.

```python
from alembic.ddl.impl import DefaultImpl

class SnowflakeImpl(DefaultImpl):
    __dialect__ = 'snowflake'
```

See [Alembic Documentation](http://alembic.zzzcomputing.com/) for general usage.

### Key Pair Authentication Support

Snowflake SQLAlchemy supports key pair authentication by leveraging its Snowflake Connector for Python underpinnings. See [Using Key Pair Authentication](https://docs.snowflake.net/manuals/user-guide/python-connector-example.html#using-key-pair-authentication) for steps to create the private and public keys.

The private key parameter is passed through `connect_args` as follows:

```python
...
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

with open("rsa_key.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
        backend=default_backend()
    )

pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())

engine = create_engine(URL(
    account='abc123',
    user='testuser1',
    ),
    connect_args={
        'private_key': pkb,
        },
    )
```

Where `PRIVATE_KEY_PASSPHRASE` is a passphrase to decrypt the private key file, `rsa_key.p8`.

Currently a private key parameter is not accepted by the `snowflake.sqlalchemy.URL` method.

### Merge Command Support

Snowflake SQLAlchemy supports upserting with its `MergeInto` custom expression.
See [Merge](https://docs.snowflake.net/manuals/sql-reference/sql/merge.html)  for full documentation.

Use it as follows:

```python
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, create_engine
from snowflake.sqlalchemy import MergeInto

engine = create_engine(db.url, echo=False)
session = sessionmaker(bind=engine)()
connection = engine.connect()

meta = MetaData()
meta.reflect(bind=session.bind)
t1 = meta.tables['t1']
t2 = meta.tables['t2']

merge = MergeInto(target=t1, source=t2, on=t1.c.t1key == t2.c.t2key)
merge.when_matched_then_delete().where(t2.c.marked == 1)
merge.when_matched_then_update().where(t2.c.isnewstatus == 1).values(val = t2.c.newval, status=t2.c.newstatus)
merge.when_matched_then_update().values(val=t2.c.newval)
merge.when_not_matched_then_insert().values(val=t2.c.newval, status=t2.c.newstatus)
connection.execute(merge)
```

### CopyIntoStorage Support

Snowflake SQLAlchemy supports saving tables/query results into different stages, as well as into Azure Containers and
AWS buckets with its custom `CopyIntoStorage` expression. See [Copy into](https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html)
for full documentation.

Use it as follows:

```python
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, create_engine
from snowflake.sqlalchemy import CopyIntoStorage, AWSBucket, CSVFormatter

engine = create_engine(db.url, echo=False)
session = sessionmaker(bind=engine)()
connection = engine.connect()

meta = MetaData()
meta.reflect(bind=session.bind)
users = meta.tables['users']

copy_into = CopyIntoStorage(from_=users,
                            into=AWSBucket.from_uri('s3://my_private_backup').encryption_aws_sse_kms('1234abcd-12ab-34cd-56ef-1234567890ab'),
                            formatter=CSVFormatter().null_if(['null', 'Null']))
connection.execute(copy_into)
```

### Iceberg Table with Snowflake Catalog support

Snowflake SQLAlchemy supports Iceberg Tables with the Snowflake Catalog, along with various related parameters. For detailed information about Iceberg Tables, refer to the Snowflake [CREATE ICEBERG](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake) documentation.

To create an Iceberg Table using Snowflake SQLAlchemy, you can define the table using the SQLAlchemy Core syntax as follows:

```python
table = IcebergTable(
    "myuser",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    external_volume=external_volume_name,
    base_location="my_iceberg_table",
    as_query="SELECT * FROM table"
)
```

Alternatively, you can define the table using a declarative approach:

```python
class MyUser(Base):
    __tablename__ = "myuser"

    @classmethod
    def __table_cls__(cls, name, metadata, *arg, **kw):
        return IcebergTable(name, metadata, *arg, **kw)

    __table_args__ = {
        "external_volume": "my_external_volume",
        "base_location": "my_iceberg_table",
        "as_query": "SELECT * FROM table",
    }

    id = Column(Integer, primary_key=True)
    name = Column(String)
```

### Hybrid Table support

Snowflake SQLAlchemy supports Hybrid Tables with indexes. For detailed information, refer to the Snowflake [CREATE HYBRID TABLE](https://docs.snowflake.com/en/sql-reference/sql/create-hybrid-table) documentation.

To create a Hybrid Table and add an index, you can use the SQLAlchemy Core syntax as follows:

```python
table = HybridTable(
    "myuser",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Index("idx_name", "name")
)
```

Alternatively, you can define the table using the declarative approach:

```python
class MyUser(Base):
    __tablename__ = "myuser"

    @classmethod
    def __table_cls__(cls, name, metadata, *arg, **kw):
        return HybridTable(name, metadata, *arg, **kw)

    __table_args__ = (
        Index("idx_name", "name"),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String)
```

### Dynamic Tables support

Snowflake SQLAlchemy supports Dynamic Tables. For detailed information, refer to the Snowflake [CREATE DYNAMIC TABLE](https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table) documentation.

To create a Dynamic Table, you can use the SQLAlchemy Core syntax as follows:

```python
dynamic_test_table_1 = DynamicTable(
    "dynamic_MyUser",
    metadata,
    Column("id", Integer),
    Column("name", String),
    target_lag=(1, TimeUnit.HOURS),  # Additionally, you can use SnowflakeKeyword.DOWNSTREAM
    warehouse='test_wh',
    refresh_mode=SnowflakeKeyword.FULL,
    as_query="SELECT id, name from MyUser;"
)
```

Alternatively, you can define a table without columns using the SQLAlchemy `select()` construct:

```python
dynamic_test_table_1 = DynamicTable(
    "dynamic_MyUser",
    metadata,
    target_lag=(1, TimeUnit.HOURS),
    warehouse='test_wh',
    refresh_mode=SnowflakeKeyword.FULL,
    as_query=select(MyUser.id, MyUser.name)
)
```

### Notes

- Defining a primary key in a Dynamic Table is not supported, meaning declarative tables don’t support Dynamic Tables.
- When using the `as_query` parameter with a string, you must explicitly define the columns. However, if you use the SQLAlchemy `select()` construct, you don’t need to explicitly define the columns.
- Direct data insertion into Dynamic Tables is not supported.


## Verifying Package Signatures

To ensure the authenticity and integrity of the Python package, follow the steps below to verify the package signature using `cosign`.

**Steps to verify the signature:**
- Install cosign:
  - This example is using golang installation: [installing-cosign-with-go](https://edu.chainguard.dev/open-source/sigstore/cosign/how-to-install-cosign/#installing-cosign-with-go)
- Download the file from the repository like pypi:
  - https://pypi.org/project/snowflake-sqlalchemy/#files
- Download the signature files from the release tag, replace the version number with the version you are verifying:
  - https://github.com/snowflakedb/snowflake-sqlalchemy/releases/tag/v1.7.3
- Verify signature:
  ````bash
  # replace the version number with the version you are verifying
  ./cosign verify-blob snowflake_sqlalchemy-1.7.3-py3-none-any.whl  \
  --certificate snowflake_sqlalchemy-1.7.3-py3-none-any.whl.crt \
  --certificate-identity https://github.com/snowflakedb/snowflake-sqlalchemy/.github/workflows/python-publish.yml@refs/tags/v1.7.3 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --signature snowflake_sqlalchemy-1.7.3-py3-none-any.whl.sig
  Verified OK
  ````

## Support

Feel free to file an issue or submit a PR here for general cases. For official support, contact Snowflake support at:
<https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge>

## Known Limitations

### Identity columns as primary keys

Using SQLAlchemy's `Identity()` construct on a primary key column is **not compatible with the SQLAlchemy ORM** when targeting Snowflake.

**Why it fails:** After an `INSERT`, the ORM must retrieve the generated primary key to populate the in-memory object. SQLAlchemy supports two mechanisms for this — `RETURNING` (not available in Snowflake) and `cursor.lastrowid` (the Snowflake Python connector returns `None` for this attribute because Snowflake has no native rowid concept — see [snowflake-connector-python#1201](https://github.com/snowflakedb/snowflake-connector-python/pull/1201)). With neither mechanism available, the ORM receives `None` as the primary key and raises:

```
sqlalchemy.orm.exc.FlushError: Instance <MyModel at 0x...> has a NULL identity key after a flush ...
```

**Example that fails:**

```python
from sqlalchemy import Column, Identity, Integer, String
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class MyModel(Base):
    __tablename__ = "my_model"
    id = Column(Integer, Identity(start=1, increment=1), primary_key=True)  # does not work with ORM
    name = Column(String)

Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(MyModel(name="test"))
    session.commit()  # raises FlushError: NULL identity key
```

The dialect emits a `SnowflakeWarning` at DDL compile time when `Identity()` is detected on a primary key column to surface this problem early. The warning is emitted **once per unique `(table, column)` pair per Python process** — repeated DDL compilation of the same schema does not produce duplicate output.

To silence the warning entirely, use Python's standard warning filter:

```python
import warnings
from snowflake.sqlalchemy.exc import SnowflakeWarning

warnings.filterwarnings("ignore", category=SnowflakeWarning)
```

Or set it via the `PYTHONWARNINGS` environment variable before starting your application:

```shell
PYTHONWARNINGS=ignore::snowflake.sqlalchemy.exc.SnowflakeWarning python my_app.py
```

**Workaround — use `Sequence()` instead:**

```python
from sqlalchemy import Column, Integer, Sequence, String
from sqlalchemy.orm import declarative_base, Session

Base = declarative_base()

class MyModel(Base):
    __tablename__ = "my_model"
    id = Column(Integer, Sequence("my_model_id_seq"), primary_key=True)
    name = Column(String)

Base.metadata.create_all(engine)

with Session(engine) as session:
    session.add(MyModel(name="test"))
    session.commit()  # id is populated correctly
```

`Sequence` objects are fully supported by the Snowflake dialect and are the recommended way to generate auto-incrementing primary keys when using the ORM. See the [Auto-increment Behavior](#auto-increment-behavior) section for more details.

---

### Case-sensitive identifiers

Snowflake stores unquoted identifiers in UPPERCASE and treats them case-insensitively.  SQLAlchemy uses lowercase for case-insensitive identifiers.  The dialect bridges this gap via `normalize_name` / `denormalize_name`, but a few edge cases require explicit opt-in.

#### How reflection maps Snowflake names to SQLAlchemy names

When the dialect reflects a table, each column name passes through `normalize_name`, which produces one of three outcomes depending on how the identifier was stored in Snowflake:

| Snowflake stored form | How it was created | `normalize_name` returns | SQLAlchemy treats it as |
|---|---|---|---|
| `MYCOL` (all-uppercase) | `CREATE TABLE t (MYCOL INT)` — unquoted | `"mycol"` (plain `str`) | case-insensitive |
| `mycol` (lowercase) | `CREATE TABLE t ("mycol" INT)` — quoted | `quoted_name("mycol", True)` | case-sensitive |
| `MyCol` (mixed-case) | `CREATE TABLE t ("MyCol" INT)` — quoted | `"MyCol"` (plain `str`) | case-sensitive via preparer |

You can observe this directly via the inspector:

```python
from sqlalchemy import inspect
from sqlalchemy.sql.elements import quoted_name

inspector = inspect(engine)
for col in inspector.get_columns("my_table"):
    name = col["name"]
    if isinstance(name, quoted_name) and name.quote:
        print(f"{name!r} — case-sensitive (was created quoted in Snowflake)")
    else:
        print(f"{name!r} — case-insensitive (Snowflake stores as {name.upper()})")
```

This means that after reflection, accessing a case-insensitive column requires the lowercase name:

```python
metadata.reflect(bind=engine)
t = metadata.tables["my_table"]
t.c.mycol      # correct — Snowflake stored MYCOL, reflected as "mycol"
t.c["MYCOL"]   # KeyError — the reflected key is lowercase
```

And a case-sensitive column (quoted in Snowflake) is accessed by its exact reflected name:

```python
t.c[quoted_name("mycol", True)]  # correct — matches the reflected quoted_name key
t.c.mycol                        # also works — quoted_name.__eq__ compares by value
```

#### Lowercase column names in CLUSTER BY

Column objects wrapped in `quoted_name("mycol", True)` are treated as case-sensitive by SQLAlchemy.  Pass them directly to `snowflake_clusterby`:

```python
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy import Column, Integer, MetaData, Table

t = Table(
    "my_table",
    MetaData(),
    Column(quoted_name("mycol", True), Integer),
    snowflake_clusterby=[quoted_name("mycol", True)],
)
# Generates: CLUSTER BY ("mycol")
```

Without `quoted_name(..., True)` the column name is treated as case-insensitive and Snowflake resolves it as `MYCOL`.

#### ALL-UPPERCASE identifiers that are SQL reserved words (e.g. `TABLE`, `SELECT`)

Most identifiers are handled correctly without any flag:

- A quoted lowercase name (`"mycol"` in Snowflake) reflects as `quoted_name("mycol", True)` — already case-sensitive.
- A quoted mixed-case name (`"MyCol"`) reflects as the plain string `"MyCol"` — already quoted by the preparer.
- An unquoted name (`MYCOL` stored as all-uppercase) reflects as `"mycol"` — correctly case-insensitive.

The one gap is an identifier whose all-uppercase form is also a SQL reserved word.  For example, a table literally named `TABLE` (created as `CREATE TABLE "TABLE" ...`) stores as `TABLE` in Snowflake.  When the dialect reflects it, `normalize_name("TABLE")` cannot tell whether `TABLE` is a plain uppercase column or the keyword `TABLE`, so by default it returns `"TABLE"` unchanged — an all-uppercase string that SQLAlchemy treats as case-sensitive.  This causes a key mismatch: the table was reflected under the key `"TABLE"` but the same `normalize_name` call made during DDL would produce `"table"`.

Enable `case_sensitive_identifiers` to fix this: the dialect will return `quoted_name("table", True)` for any all-uppercase identifier that is a reserved word, matching the standard SQLAlchemy convention:

```python
from sqlalchemy import create_engine

engine = create_engine(
    "snowflake://user:pass@account/db",
    case_sensitive_identifiers=True,
)
```

Or via URL:

```
snowflake://user:pass@account/db?case_sensitive_identifiers=True
```

**Hard limit:** Enabling this flag changes the dict key used by `normalize_name("TABLE")` from `"TABLE"` to `quoted_name("table", True)`.  Because `hash("TABLE") != hash("table")`, any existing code that accesses `metadata.tables["TABLE"]` by the uppercase key will miss after the flag is enabled.  Enabling the flag on an existing codebase requires auditing all string-keyed lookups into `metadata.tables` and `table.c` for names that happen to be reserved words.

#### Case-sensitive schema names

Use `create_snowflake_engine` to connect to a schema whose name is lowercase or mixed-case in Snowflake (i.e. created with double-quotes).  It handles the encoding automatically:

```python
from snowflake.sqlalchemy import create_snowflake_engine

engine = create_snowflake_engine(
    "snowflake://user:pass@account/mydb",
    schema="myschema",
    case_sensitive_schema=True,
)
```

**When the URL is stored outside Python code** (environment variable, `alembic.ini`, Docker/Kubernetes config), `create_snowflake_engine` is not available and the schema must be percent-encoded directly in the URL string.  Wrap the schema name in `%22` (the percent-encoded form of `"`):

```
snowflake://user:pass@account/mydb/%22myschema%22
```

The dialect decodes `%22` back to a literal `"` before passing the value to the Snowflake connector, which then executes `USE SCHEMA "myschema"` preserving case.

#### Alembic — hand-written migrations

`quoted_name` works directly in `op.create_table` and `op.add_column`, so hand-written migration files need no special handling:

```python
import sqlalchemy as sa
from alembic import op
from sqlalchemy.sql.elements import quoted_name

def upgrade():
    op.create_table(
        "my_table",
        sa.Column(quoted_name("mycol", True), sa.String()),  # quoted → "mycol" in Snowflake
        sa.Column("name", sa.String()),                      # unquoted → NAME in Snowflake
    )

    op.add_column(
        "my_table",
        sa.Column(quoted_name("extra", True), sa.Integer()),
    )
```

#### Alembic — case-sensitive schema configuration

When the Alembic version table or the migrations themselves target a case-sensitive schema, use the `%22` form in every place Alembic receives a schema string, because these values are passed as plain strings and `create_snowflake_engine` is not available at that point:

```python
# alembic/env.py
from sqlalchemy import create_engine

url = "snowflake://user:pass@account/mydb/%22myschema%22"

context.configure(
    url=url,
    target_metadata=target_metadata,
    version_table_schema="%22myschema%22",  # keeps alembic_version table in the same schema
)
```

#### Alembic — autogenerate and case-sensitive columns

Alembic's default renderer serialises `quoted_name("mycol", True)` as the plain string `"mycol"`, losing the case-sensitivity signal.  The generated migration would create a case-insensitive `MYCOL` column instead of `"mycol"`.

This also affects the **comparison phase**: when autogenerate detects that a reflected `quoted_name("mycol", True)` column differs from what it would render, it may emit a spurious `alter_column` on every run.  The fix for both problems is the same — register the `render_item` hook in `env.py`:

```python
from snowflake.sqlalchemy.alembic_util import render_item as snowflake_render_item

context.configure(
    ...,
    render_item=snowflake_render_item,
)
```

**Hard limit:** Alembic has no dialect-level rendering hook.  The `render_item` callback in `env.py` is the only injection point and requires a two-line opt-in **per project**.  This cannot be eliminated without upstream Alembic changes.
