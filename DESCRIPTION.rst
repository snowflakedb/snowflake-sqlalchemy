This package includes the Snowflake SQLAlchemy, which supports Snowsql dialects for SQLAlchemy 
http://www.sqlalchemy.org/

Snowflake Documentation is available at:
https://docs.snowflake.net/

Source code is also available at:
https://github.com/snowflakedb/snowflake-sqlalchemy

Release Notes
-------------------------------------------------------------------------------

- v1.1.14 (August 12, 2019)

    - Fix CSVFormatter class has `FIELD_DELIMETER` spelled incorrectly

- v1.1.13 (May 20,2019)

    - CopyInto's maxfilesize method expects a bool instead of an int
    - CopyInto statement doesn't compile correctly when the source is storage and the destination is a table

- v1.1.12 (April 8,2019)

    - Add ability to inspect column comments
    - Restricting index creation checking to only SnowflakeDialect tables

- v1.1.11 (March 25, 2019)

    - Remove relative reference to connector from SQLAlchemy dialect

- v1.1.10 (February 22, 2019)

    - Separated base.py file into smaller files and fixed import statements
    - Prevent creating tables with indexes in SQLAlchemy
    - Add tox support

- v1.1.9 (February 11, 2019)

    - Fix an issue in v1.1.8 

- v1.1.8 (February 8, 2019)

    - Fixed a dependency

- v1.1.7 (February 8, 2019)
		
    - Added Upsert in sql-alchemy
    - CopyIntoS3 command in SQLAlchemy

- v1.1.6 (January 3, 2019)

    - Fixed 'module' object is not callable in csvsql

- v1.1.5 (December 19, 2018)

    - Added multivalue_support feature flag
    - Deprecate get_primary_keys

- v1.1.4 (November 13, 2018)

    - Fixed lable/alias by honoring quote_name.

- v1.1.3 (October 30, 2018)

    - SQLAlchemy 1.2 multi table support.
    - TIMESTAMP_LTZ, TIMESTAMP_NTZ and TIMESTAMP_TZ support.
    - Fixed relative import issue in SQLAlchemy

- v1.1.2 (June 7, 2018)

    - Removes username restriction for OAuth

- v1.1.1 (May 17, 2018)

    - Made password as optional parameter for SSO support
    - Fixed paramstyl=qmark mode where the data are bound in the server instead of client side
    - Fixed multipart schema support. Now db.schema can be specified in the schema parameters.
    - Added ``region`` parameter support to ``URL`` utility method.

- v1.1.0 (February 1, 2018)

    - Updated doc including ``role`` example.
    - Fixed the return value of ``get_pk_constraint`` and ``get_primary_keys``. Those applications that depend on the old behaviors must update codes. Issue #38 (@nrth)
    - Updated doc including a note about ``open`` and ``close`` connections.

- v1.0.9 (January 4, 2018)

    - Fixed foreign key names that should be normalized. Issue #24 (@cladden)
    - Set the default schema Issue #25 (@cladden)
    - Improved performance by caching current database and schema for inspector. Issue #30 (@cladden)

- v1.0.8 (December 21, 2017)

    - Added ``get_schema_names`` method to Snowflake SQLAlchemy dialect. PR #20(andrewsali)
    - Fixed the column metadata including length for string/varchar and precision and scale for numeric data type. Issue #22(@cladden)

- v1.0.7 (May 18, 2017)

    - Fixed COPY command transaction issue. PR #16(Pangstar) and Issue #17(Pangstar)

- v1.0.6 (April 20, 2017)

    - Fixed account with subdomain issue. Issue #15(Pangstar)

- v1.0.5 (April 13, 2017)

    - Added ``snowflake_clusterby`` option support to ``Table`` object so that the user can create a table with clustering keys

- v1.0.4 (March 9, 2017)

    - Added SQLAlchemy 1.1 support

- v1.0.3 (October 20, 2016)

    - Added ``VARIANT``, ``OBJECT`` and ``ARRAY`` data type supports for fetch

- v1.0.2 (July 5, 2016)

    - Fixed the development status in classifiers. 5 - Production/Stable

- v1.0.1 (July 4, 2016)

    - Fixed URL method in case of including warehouse without database.

- v1.0.0 (June 28, 2016)

    - General Availability
