This package includes the Snowflake SQLAlchemy, which supports Snowsql dialects for SQLAlchemy 
http://www.sqlalchemy.org/

Snowflake Documentation is available at:
https://docs.snowflake.net/

Source code is also available at:
https://github.com/snowflakedb/snowflake-sqlalchemy

Release Notes
-------------------------------------------------------------------------------

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
