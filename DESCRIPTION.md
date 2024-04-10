This package includes the Snowflake SQLAlchemy, which supports Snowsql dialects for SQLAlchemy
<http://www.sqlalchemy.org/>

Snowflake Documentation is available at:
<https://docs.snowflake.net/>

Source code is also available at:
<https://github.com/snowflakedb/snowflake-sqlalchemy>

# Release Notes

- v1.6.0(Not released)

  - support for installing with SQLAlchemy 2.0.x

- v1.5.3(Not relased)

    - Limit SQLAlchemy to < 2.0.0 before releasing version compatible with 2.0

- v1.5.2(April 11, 2024)

  - Bump min SQLAlchemy to 1.4.19 for outer lateral join
  - Add support for sequence ordering in tests

- v1.5.1(November 03, 2023)

  - Fixed a compatibility issue with Snowflake Behavioral Change 1057 on outer lateral join, for more details check https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057.
  - Fixed credentials with `externalbrowser` authentication not caching due to incorrect parsing of boolean query parameters.
    - This fixes other boolean parameter passing to driver as well.

- v1.5.0(Aug 23, 2023)

  - Added option to create a temporary stage command.
  - Added support for geometry type.
  - Fixed a compatibility issue of regex expression with SQLAlchemy 1.4.49.

- v1.4.7(Mar 22, 2023)

  - Re-applied the application name of driver connection `SnowflakeConnection` to `SnowflakeSQLAlchemy`.
  - `SnowflakeDialect.get_columns` now throws a `NoSuchTableError` exception when the specified table doesn't exist, instead of the more vague `KeyError`.
  - Fixed a bug that dialect can not be created with empty host name.
  - Fixed a bug that `sqlalchemy.func.now` is not rendered correctly.

- v1.4.6(Feb 8, 2023)

  - Bumped snowflake-connector-python dependency to newest version which supports Python 3.11.
  - Reverted the change of application name introduced in v1.4.5 until support gets added.

- v1.4.5(Dec 7, 2022)

  - Updated the application name of driver connection `SnowflakeConnection` to `SnowflakeSQLAlchemy`.

- v1.4.4(Nov 16, 2022)

  - Fixed a bug that percent signs in a non-compiled statement should not be interpolated with emtpy sequence when executed.

- v1.4.3(Oct 17, 2022)

  - Fixed a bug that `SnowflakeDialect.normalize_name` and `SnowflakeDialect.denormalize_name` could not handle empty string.
  - Fixed a compatibility issue to vendor function `sqlalchemy.engine.url._rfc_1738_quote` as it is removed from SQLAlchemy v1.4.42.

- v1.4.2(Sep 19, 2022)

  - Improved performance by standardizing string interpolations to f-strings.
  - Improved reliability by always using context managers.

- v1.4.1(Aug 18, 2022)

  - snowflake-sqlalchemy is now SQLAlchemy 2.0 compatible.
  - Fixed a bug that `DATE` should not be removed from `SnowflakeDialect.ischema_names`.
  - Fixed breaking changes introduced in release 1.4.0 that:
    - changed the behavior of processing numeric, datetime and timestamp values returned from service.
    - changed the sequence order of primary/foreign keys in list returned by `inspect.get_foreign_keys` and `inspect.get_pk_constraint`.

- v1.4.0(July 20, 2022)

  - Added support for `regexp_match`, `regexp_replace` in `sqlalchemy.sql.expression.ColumnOperators`.
  - Added support for Identity Column.
  - Added support for handling literals value of sql type `Date`, `DateTime`, `Time`, `Float` and `Numeric`, and converting the values into corresponding Python objects.
  - Added support for `get_sequence_names` in `SnowflakeDialect`.
  - Fixed a bug where insert with autoincrement failed due to incompatible column type affinity #124.
  - Fixed a bug when creating a column with sequence, default value was set incorrectly.
  - Fixed a bug that identifier having percents in a compiled statement was not interpolated.
  - Fixed a bug when visiting sequence value from another schema, the sequence name is not formatted with the schema name.
  - Fixed a bug where the sequence order of columns were not maintained when retrieving primary keys and foreign keys for a table.

- v1.3.4(April 27,2022)

  - Fixed a bug where identifier max length was set to the wrong value and added relevant schema introspection
  - Add support for geography type
  - Fixed a bug where foreign key's referred schema was set incorrectly
  - Disabled new SQLAlchemy option for statement caching until support gets added

- v1.3.3(December 19,2021)

  - Fixed an issue where quote arguments were stripped from identifiers.

- v1.3.2 (September 14,2021)

  - Fixed a breaking change introduced in SQLAlchemy 1.4 that changed the behavior of returns_unicode_strings.

- v1.3.1 (July 23,2021)

  - Raising minimum version of SQLAlchemy to match used features.

- v1.2.5 (July 20,2021)

  - Various custom command bug fixes and additions.

- v1.2.4 (October 05,2020)

  - Fixed an issue where inspector would not properly switch to table wide column retrieving when schema wide column retrieving was taking too long to respond.

- v1.2.3 (March 30, 2020)

  - Update tox.ini
  - Add external stage to COPY INTO custom command.
  - Bumped pandas to newest versions

- v1.2.2 (March 9, 2020)

  - Allow get_table_comment to fetch view comments too

- v1.2.1 (February 18,2020)

  - Add driver property to SnowflakeDialect #140
  - Suppress deprecation warning by fixing import

- v1.2.0 (January 27, 2020)

  - Fix typo in README Connection Parameters #141
  - Fix sqlalchemy and possibly python-connector warnings
  - Fix handling of empty table comments #137
  - Fix handling spaces in connection string passwords #149

- v1.1.18 (January 6,2020)

  - Set current schema in connection string containing special characters
  - Calling str on custom_types throws Exception

- v1.1.17 (December 2,2019)

  - Comments not created when creating new table #118
  - SQLAlchemy Column Metadata Cache not working
  - Timestamp DDL renders wrong when precision value passed
  - Fixed special character handling in snowflake-sqlalchemy from URL string
  - Added development optional dependencies to Python packages

- v1.1.16 (October 21,2019)

  - Fix SQLAlchemy not working with global url

- v1.1.15 (September 30, 2019)

  - Incorrect SQL generated for INSERT with CTE
  - Type Synonyms not exported to top-level module #109

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
