#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):

    # stay-closed properties
    # - autocommit: TODO: confirm sqlalchemy's autocommit isolation level concept does not apply to snowflake based on the doc
    # - isolation_level: snowflake only supports read committed
    # https://docs.snowflake.com/en/sql-reference/transactions.html#label-txn-autocommit
    # https://docs.sqlalchemy.org/en/14/core/connections.html#setting-transaction-isolation-levels-including-dbapi-autocommit
    # - index_ddl_if_exists: index not supported in snowflake
    # - non_updating_cascade: updating cascade supported
    # - empty_inserts: not supported in snowflake
    # - full_returning: not supported in snowflake
    # - insert_executemany_returning: not supported in snowflake
    # - returning: not supported in snowflake
    # - supports_lastrowid: TODO: not supported, check SNOW-11155
    # - indexes_with_expressions: index not supported in snowflake
    # - check_constraint_reflection: not supported in snowflake
    # - reflect_tables_no_columns: not supported in snowflake
    # - server_side_cursors: no supported in snowflake
    # - foreign_key_constraint_option_reflection_ondelete: TODO, check service side issue or by design?
    # - fk_constraint_option_reflection_ondelete_restrict: TODO, check service side issue or by design?
    # - foreign_key_constraint_option_reflection_onupdate: TODO, check service side issue or by design?
    # - fk_constraint_option_reflection_onupdate_restrict: TODO, check service side issue or by design?
    # - index_reflects_included_columns: index not supported in snowflake
    # - dbapi_lastrowid: TODO, not supported in snowflake python connector, support it in the future
    # - savepoints: not supported in snowflake
    # - two_phase_transactions: not supported in snowflake
    # - async_dialect: no await used
    # - computed_columns: TODO: not supported in snowflake yet, check SNOW-169530 for virtual column
    # - computed_columns_default_persisted: TODO: not supported in snowflake yet, check SNOW-169530 for virtual column
    # - computed_columns_reflect_persisted: TODO: not supported in snowflake yet, check SNOW-169530 for virtual column
    # - computed_columns_virtual: TODO: not supported in snowflake yet, check SNOW-169530 for virtual column
    # - computed_columns_stored: TODO: not supported in snowflake yet, check SNOW-169530 for virtual column
    # - fetch_expression: not supported in snowflake
    # - fetch_percent: not supported in snowflake
    # - fetch_ties: not supported in snowflake
    # - supports_distinct_on: not supported in snowflake
    # - identity_columns: not supported in snowflake
    # - identity_columns_standard: not supported in snowflake

    @property
    def table_ddl_if_exists(self):
        return exclusions.open()

    @property
    def table_value_constructor(self):
        return exclusions.open()

    @property
    def deferrable_fks(self):
        return exclusions.open()

    @property
    def boolean_col_expressions(self):
        return exclusions.open()

    @property
    def nullsordering(self):
        return exclusions.open()

    @property
    def standalone_binds(self):
        return exclusions.open()

    @property
    def intersect(self):
        return exclusions.open()

    @property
    def except_(self):
        return exclusions.open()

    @property
    def window_functions(self):
        return exclusions.open()

    @property
    def ctes(self):
        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        # Snowflake CTE could only be followed by SELECT
        # https://docs.snowflake.com/en/user-guide/queries-cte.html
        return exclusions.closed()

    @property
    def ctes_on_dml(self):
        return exclusions.open()

    @property
    def tuple_in(self):
        return exclusions.open()

    @property
    def emulated_lastrowid(self):
        return exclusions.open()

    @property
    def emulated_lastrowid_even_with_sequences(self):
        return exclusions.open()

    @property
    def views(self):
        return exclusions.open()

    @property
    def cross_schema_fk_reflection(self):
        return exclusions.open()

    @property
    def foreign_key_constraint_name_reflection(self):
        return exclusions.open()

    @property
    def implicit_default_schema(self):
        return exclusions.open()

    @property
    def default_schema_name_switch(self):
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        return exclusions.open()

    @property
    def comment_reflection(self):
        return exclusions.open()

    @property
    def fk_constraint_option_reflection_ondelete_noaction(self):
        return exclusions.open()

    @property
    def temp_table_names(self):
        return exclusions.open()

    @property
    def temporary_views(self):
        return exclusions.open()

    @property
    def unicode_ddl(self):
        return exclusions.open()

    @property
    def datetime_literals(self):
        return exclusions.open()

    @property
    def datetime_timezone(self):
        # by default snowflake uses TIMESTAMP_NTZ which contains no time zone info
        # TODO: consider creating a new column type TIMESTAMP_TZ for the the time zone info
        # https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
        return exclusions.closed()

    @property
    def time_timezone(self):
        # not supported in snowflake
        # TODO: consider creating a new column type TIMEZONE_TZ which uses TIMESTAMP_TZ for the time zone info
        # https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#time
        return exclusions.closed()

    @property
    def timestamp_microseconds(self):
        return exclusions.open()

    @property
    def datetime_historic(self):
        return exclusions.open()

    @property
    def date_historic(self):
        return exclusions.open()

    @property
    def json_type(self):
        # TODO: need service/connector support
        # check https://snowflakecomputing.atlassian.net/browse/SNOW-52370
        return exclusions.closed()

    @property
    def legacy_unconditional_json_extract(self):
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        return exclusions.open()

    @property
    def precision_numerics_many_significant_digits(self):
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self):
        return exclusions.open()

    @property
    def infinity_floats(self):
        return exclusions.open()

    @property
    def update_from(self):
        return exclusions.open()

    @property
    def delete_from(self):
        return exclusions.open()

    @property
    def mod_operator_as_percent_sign(self):
        return exclusions.open()

    @property
    def percent_schema_names(self):
        return exclusions.open()

    @property
    def order_by_label_with_expression(self):
        return exclusions.open()

    @property
    def regexp_match(self):
        return exclusions.open()

    @property
    def regexp_replace(self):
        return exclusions.open()

    @property
    def fetch_first(self):
        return exclusions.open()

    @property
    def fetch_no_order_by(self):
        return exclusions.open()

    @property
    def fetch_offset_with_options(self):
        return exclusions.open()

    @property
    def sql_expression_limit_offset(self):
        # Snowflake only takes non-negative integer constants for offset/limit
        return exclusions.closed()

    @property
    def implements_get_lastrowid(self):
        # TODO: need connector lastrowid support, check SNOW-11155
        return exclusions.closed()
