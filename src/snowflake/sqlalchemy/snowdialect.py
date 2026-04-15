#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import decimal
import operator
from collections import defaultdict
from enum import Enum
from functools import reduce
from logging import getLogger
from time import time as time_in_seconds
from typing import Any, Collection, Optional, cast
from urllib.parse import unquote_plus

import sqlalchemy.sql.sqltypes as sqltypes
from sqlalchemy import __version__ as SQLALCHEMY_VERSION
from sqlalchemy import event as sa_vnt
from sqlalchemy import exc as sa_exc
from sqlalchemy import util as sa_util
from sqlalchemy.engine import URL, default, reflection
from sqlalchemy.schema import Table
from sqlalchemy.sql import text
from sqlalchemy.sql.sqltypes import NullType
from sqlalchemy.types import FLOAT, Date, DateTime, Float, Time

from snowflake.connector import errors as sf_errors
from snowflake.connector.connection import DEFAULT_CONFIGURATION, SnowflakeConnection
from snowflake.connector.constants import UTF8
from snowflake.connector.telemetry import TelemetryClient, TelemetryData, TelemetryField
from snowflake.sqlalchemy.compat import IS_VERSION_20, returns_unicode
from snowflake.sqlalchemy.name_utils import _NameUtils
from snowflake.sqlalchemy.structured_type_info_manager import _StructuredTypeInfoManager

from ._constants import DIALECT_NAME
from .base import (
    SnowflakeCompiler,
    SnowflakeDDLCompiler,
    SnowflakeExecutionContext,
    SnowflakeIdentifierPreparer,
    SnowflakeTypeCompiler,
)
from .custom_types import (
    DECFLOAT_PRECISION,
    VECTOR,
    StructuredType,
    _CUSTOM_Date,
    _CUSTOM_DateTime,
    _CUSTOM_Float,
    _CUSTOM_Time,
)
from .parser.custom_type_parser import *  # noqa
from .parser.custom_type_parser import _CUSTOM_DECIMAL  # noqa
from .parser.custom_type_parser import ischema_names, parse_index_columns, parse_type
from .sql.custom_schema.custom_table_prefix import CustomTablePrefix
from .util import (
    _update_connection_application_name,
    parse_url_boolean,
    parse_url_integer,
)

colspecs = {
    Date: _CUSTOM_Date,
    DateTime: _CUSTOM_DateTime,
    Time: _CUSTOM_Time,
    Float: _CUSTOM_Float,
}

_ENABLE_SQLALCHEMY_AS_APPLICATION_NAME = True

logger = getLogger(__name__)


class TelemetryEvents(Enum):
    NEW_CONNECTION = "sqlalchemy_new_connection"


class SnowflakeIsolationLevel(Enum):
    READ_COMMITTED = "READ COMMITTED"
    AUTOCOMMIT = "AUTOCOMMIT"


class SnowflakeDialect(default.DefaultDialect):
    name = DIALECT_NAME
    driver = "snowflake"
    max_identifier_length = 255
    cte_follows_insert = True

    # TODO: support SQL caching, for more info see: https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    supports_statement_cache = False

    encoding = UTF8
    default_paramstyle = "pyformat"
    colspecs = colspecs
    ischema_names = ischema_names

    # target database treats the / division operator as “floor division”
    div_is_floordiv = False

    # all str types must be converted in Unicode
    convert_unicode = True

    # Indicate whether the DB-API can receive SQL statements as Python
    #  unicode strings
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = returns_unicode
    description_encoding = None

    # No lastrowid support. See SNOW-11155
    postfetch_lastrowid = False

    # Indicate whether the dialect properly implements rowcount for
    #  ``UPDATE`` and ``DELETE`` statements.
    supports_sane_rowcount = True

    # Indicate whether the dialect properly implements rowcount for
    # ``UPDATE`` and ``DELETE`` statements when executed via
    # executemany.
    supports_sane_multi_rowcount = True

    # NUMERIC type returns decimal.Decimal
    supports_native_decimal = True

    # The dialect supports a native boolean construct.
    # This will prevent types.Boolean from generating a CHECK
    # constraint when that type is used.
    supports_native_boolean = True

    # The dialect supports ``ALTER TABLE``.
    supports_alter = True

    # The dialect supports CREATE SEQUENCE or similar.
    supports_sequences = True

    # The dialect supports a native ENUM construct.
    supports_native_enum = False

    # The dialect supports inserting multiple rows at once.
    supports_multivalues_insert = True

    # The dialect supports comments
    supports_comments = True

    preparer = SnowflakeIdentifierPreparer
    ddl_compiler = SnowflakeDDLCompiler
    type_compiler = SnowflakeTypeCompiler
    statement_compiler = SnowflakeCompiler
    execution_ctx_cls = SnowflakeExecutionContext

    # indicates symbol names are UPPERCASEd if they are case insensitive
    # within the database. If this is True, the methods normalize_name()
    # and denormalize_name() must be provided.
    requires_name_normalize = True

    multivalues_inserts = True

    supports_schemas = True

    sequences_optional = True

    supports_is_distinct_from = True

    supports_identity_columns = True

    def __init__(
        self,
        force_div_is_floordiv: bool = True,
        isolation_level: Optional[str] = SnowflakeIsolationLevel.READ_COMMITTED.value,
        enable_decfloat: bool = False,
        case_sensitive_identifiers: bool = False,
        **kwargs: Any,
    ):
        super().__init__(isolation_level=isolation_level, **kwargs)
        self.force_div_is_floordiv = force_div_is_floordiv
        self.div_is_floordiv = force_div_is_floordiv
        self._case_sensitive_identifiers = case_sensitive_identifiers
        self.name_utils = _NameUtils(
            self.identifier_preparer,
            case_sensitive_identifiers=case_sensitive_identifiers,
        )
        self._enable_decfloat = enable_decfloat

    def initialize(self, connection):
        super().initialize(connection)
        self.div_is_floordiv = self.force_div_is_floordiv

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    @classmethod
    def import_dbapi(cls):
        from snowflake import connector

        return connector

    @staticmethod
    def parse_query_param_type(name: str, value: Any) -> Any:
        """Cast param value if possible to type defined in connector-python."""
        if not (maybe_type_configuration := DEFAULT_CONFIGURATION.get(name)):
            return value

        _, expected_type = maybe_type_configuration
        if not isinstance(expected_type, tuple):
            expected_type = (expected_type,)

        if isinstance(value, expected_type):
            return value

        elif bool in expected_type:
            return parse_url_boolean(value)
        elif int in expected_type:
            return parse_url_integer(value)
        else:
            return value

    def create_connect_args(self, url: URL):
        opts = url.translate_connect_args(username="user")
        if "database" in opts:
            name_spaces = [unquote_plus(e) for e in opts["database"].split("/")]
            if len(name_spaces) == 1:
                pass
            elif len(name_spaces) == 2:
                opts["database"] = name_spaces[0]
                opts["schema"] = name_spaces[1]
            else:
                raise sa_exc.ArgumentError(
                    f"Invalid name space is specified: {opts['database']}"
                )
        if (
            "host" in opts
            and ".snowflakecomputing.com" not in opts["host"]
            and not opts.get("port")
        ):
            opts["account"] = opts["host"]
            if "." in opts["account"]:
                # remove region subdomain
                opts["account"] = opts["account"][0 : opts["account"].find(".")]
                # remove external ID
                opts["account"] = opts["account"].split("-")[0]
            opts["host"] = opts["host"] + ".snowflakecomputing.com"
            opts["port"] = "443"
        opts["autocommit"] = False  # autocommit is disabled by default

        query = dict(**url.query)  # make mutable
        cache_column_metadata = query.pop("cache_column_metadata", None)
        self._cache_column_metadata = (
            parse_url_boolean(cache_column_metadata) if cache_column_metadata else False
        )

        # Handle enable_decfloat URL parameter
        enable_decfloat = query.pop("enable_decfloat", None)
        if enable_decfloat is not None:
            self._enable_decfloat = parse_url_boolean(enable_decfloat)

        # Handle case_sensitive_identifiers URL parameter
        case_sensitive_identifiers = query.pop("case_sensitive_identifiers", None)
        if case_sensitive_identifiers is not None:
            flag = parse_url_boolean(case_sensitive_identifiers)
            self._case_sensitive_identifiers = flag
            self.name_utils.case_sensitive_identifiers = flag

        # URL sets the query parameter values as strings, we need to cast to expected types when necessary
        for name, value in query.items():
            opts[name] = self.parse_query_param_type(name, value)

        return ([], opts)

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kw):
        """
        Checks if the table exists
        """
        return self._has_object(connection, "TABLE", table_name, schema)

    def get_isolation_level_values(self, dbapi_connection):
        return [
            SnowflakeIsolationLevel.READ_COMMITTED.value,
            SnowflakeIsolationLevel.AUTOCOMMIT.value,
        ]

    def do_rollback(self, dbapi_connection):
        dbapi_connection.rollback()

    def do_commit(self, dbapi_connection):
        dbapi_connection.commit()

    def get_default_isolation_level(self, dbapi_conn):
        return SnowflakeIsolationLevel.READ_COMMITTED.value

    def set_isolation_level(self, dbapi_connection, level):
        if level == SnowflakeIsolationLevel.AUTOCOMMIT.value:
            dbapi_connection.autocommit(True)
        else:
            dbapi_connection.autocommit(False)

    @reflection.cache
    def has_sequence(self, connection, sequence_name, schema=None, **kw):
        """
        Checks if the sequence exists
        """
        return self._has_object(connection, "SEQUENCE", sequence_name, schema)

    def _has_object(self, connection, object_type, object_name, schema=None):
        full_name = self._denormalize_quote_join(schema, object_name)
        try:
            results = connection.execute(
                text(f"DESC {object_type} /* sqlalchemy:_has_object */ {full_name}")
            )
            row = results.fetchone()
            have = row is not None
            return have
        except sa_exc.DBAPIError as e:
            if e.orig.__class__ == sf_errors.ProgrammingError:
                return False
            raise

    def normalize_name(self, name):
        return self.name_utils.normalize_name(name)

    def denormalize_name(self, name):
        return self.name_utils.denormalize_name(name)

    def _denormalize_quote_join(self, *idents):
        ip = self.identifier_preparer
        split_idents = reduce(
            operator.add,
            [ip._split_schema_by_dot(ids) for ids in idents if ids is not None],
        )
        return ".".join(ip._quote_free_identifiers(*split_idents))

    def _always_quote_join(self, *idents):
        """Build a dot-joined identifier string that always quotes every part.

        Unlike _denormalize_quote_join (which quotes only when _requires_quotes
        demands it), this helper denormalizes each segment first and then
        unconditionally wraps it in double-quotes.  This is safe because quoting
        a denormalized Snowflake identifier is semantically equivalent to the
        unquoted form for case-insensitive names, while also being correct for
        case-sensitive ones.

        IMPORTANT: denormalization must happen before quoting.  Quoting the
        SA-normalized (lowercase) form would produce "my_table" which Snowflake
        resolves as a case-sensitive reference to a table literally stored as
        my_table — different from the stored MY_TABLE.

        Only use this for new, single-table SQL helpers.  Existing callers of
        _denormalize_quote_join must not be changed to avoid altering SQL output
        for existing users (backward-compatibility constraint).
        """
        ip = self.identifier_preparer
        split_idents = reduce(
            operator.add,
            [ip._split_schema_by_dot(ids) for ids in idents if ids is not None],
        )
        return ".".join(ip.quote(self.denormalize_name(i)) for i in split_idents)

    def _get_full_schema_name(self, connection, schema=None, **kw):
        """
        Get fully-qualified schema name as database.schema.

        Args:
            connection: Database connection
            schema: Optional schema name. If None, uses default_schema_name.
            **kw: Keyword arguments including optional info_cache

        Returns:
            Fully-qualified schema name as "database"."schema"
        """
        schema = schema or self.default_schema_name
        current_database, current_schema = self._current_database_schema(
            connection, **kw
        )
        return self._denormalize_quote_join(
            current_database, schema if schema else current_schema
        )

    @reflection.cache
    def _current_database_schema(self, connection, **kw):
        res = connection.execute(
            text("select current_database(), current_schema();")
        ).fetchone()
        return (
            self.normalize_name(res[0]),
            self.normalize_name(res[1]),
        )

    def _get_server_version_info(self, connection):
        """Query and parse the Snowflake server version."""
        result = connection.execute(text("SELECT CURRENT_VERSION()"))
        version_row = result.fetchone()
        if version_row is None or len(version_row) == 0:
            return None
        # Split in case <internal identifier> documented in http://docs.snowflake.com/en/sql-reference/functions/current_version is added
        version = version_row[0].split()[0]
        return tuple(int(x) for x in version.split("."))

    def _get_default_schema_name(self, connection):
        # NOTE: no cache object is passed here
        _, current_schema = self._current_database_schema(connection)
        return current_schema

    @staticmethod
    def _map_name_to_idx(result):
        name_to_idx = {}
        for idx, col in enumerate(result.cursor.description):
            name_to_idx[col[0]] = idx
        return name_to_idx

    @reflection.cache
    def get_check_constraints(self, connection, table_name, schema, **kw):
        # check constraints are not supported by Snowflake
        return []

    # ---------------------------------------------------------------------------
    # Shared row-parsing helpers
    # ---------------------------------------------------------------------------

    def _parse_pk_rows(self, rows):
        """Parse SHOW PRIMARY KEYS rows into {table_name: {constrained_columns, name}}.

        Both SHOW PRIMARY KEYS IN TABLE and SHOW PRIMARY KEYS IN SCHEMA return the
        same column set (including table_name), so this helper works for both paths.
        Columns are sorted by key_sequence to preserve the constraint's declared order.
        """
        result = {}
        for row in rows:
            table_name = self.normalize_name(row._mapping["table_name"])
            if table_name not in result:
                result[table_name] = {
                    "constrained_columns": [],
                    "name": self.normalize_name(row._mapping["constraint_name"]),
                }
            result[table_name]["constrained_columns"].append(
                (
                    int(row._mapping["key_sequence"]),
                    self.normalize_name(row._mapping["column_name"]),
                )
            )
        for entry in result.values():
            entry["constrained_columns"] = [
                col for _, col in sorted(entry["constrained_columns"])
            ]
        return result

    def _parse_uk_rows(self, rows):
        """Parse SHOW UNIQUE KEYS rows into {table_name: [{column_names, name}]}.

        Both SHOW UNIQUE KEYS IN TABLE and SHOW UNIQUE KEYS IN SCHEMA return the
        same column set, so this helper works for both paths.
        Columns are sorted by key_sequence to preserve the constraint's declared order.
        """
        constraints = {}  # keyed by (table_name, constraint_name)
        for row in rows:
            table_name = self.normalize_name(row._mapping["table_name"])
            constraint_name = self.normalize_name(row._mapping["constraint_name"])
            key = (table_name, constraint_name)
            if key not in constraints:
                constraints[key] = {
                    "column_names": [
                        (
                            int(row._mapping["key_sequence"]),
                            self.normalize_name(row._mapping["column_name"]),
                        )
                    ],
                    "name": constraint_name,
                    "_table_name": table_name,
                }
            else:
                constraints[key]["column_names"].append(
                    (
                        int(row._mapping["key_sequence"]),
                        self.normalize_name(row._mapping["column_name"]),
                    )
                )
        result = defaultdict(list)
        for constraint in constraints.values():
            table_name = constraint.pop("_table_name")
            constraint["column_names"] = [
                col for _, col in sorted(constraint["column_names"])
            ]
            result[table_name].append(constraint)
        return dict(result)

    def _parse_fk_rows(self, rows, same_schemas):
        """Parse SHOW IMPORTED KEYS rows into {fk_table_name: [{...}]}.

        Both SHOW IMPORTED KEYS IN TABLE and SHOW IMPORTED KEYS IN SCHEMA return
        the same column set, so this helper works for both paths.
        Columns are sorted by key_sequence to preserve the constraint's declared order.

        same_schemas: set of normalised schema names for which referred_schema
        should be returned as None (same-schema FK, no need to qualify).  See:
        https://docs.sqlalchemy.org/en/14/core/reflection.html#reflection-schema-qualified-interaction
        """
        fk_map = {}  # keyed by fk_name
        for row in rows:
            fk_name = self.normalize_name(row._mapping["fk_name"])
            if fk_name not in fk_map:
                referred_schema = self.normalize_name(row._mapping["pk_schema_name"])
                fk_table_name = self.normalize_name(row._mapping["fk_table_name"])
                fk_map[fk_name] = {
                    "constrained_columns": [
                        (
                            int(row._mapping["key_sequence"]),
                            self.normalize_name(row._mapping["fk_column_name"]),
                        )
                    ],
                    "referred_schema": (
                        None if referred_schema in same_schemas else referred_schema
                    ),
                    "referred_table": self.normalize_name(
                        row._mapping["pk_table_name"]
                    ),
                    "referred_columns": [
                        (
                            int(row._mapping["key_sequence"]),
                            self.normalize_name(row._mapping["pk_column_name"]),
                        )
                    ],
                    "name": fk_name,
                    "_fk_table_name": fk_table_name,
                }
                options = {}
                if self.normalize_name(row._mapping["delete_rule"]) != "NO ACTION":
                    options["ondelete"] = self.normalize_name(
                        row._mapping["delete_rule"]
                    )
                if self.normalize_name(row._mapping["update_rule"]) != "NO ACTION":
                    options["onupdate"] = self.normalize_name(
                        row._mapping["update_rule"]
                    )
                fk_map[fk_name]["options"] = options
            else:
                fk_map[fk_name]["constrained_columns"].append(
                    (
                        int(row._mapping["key_sequence"]),
                        self.normalize_name(row._mapping["fk_column_name"]),
                    )
                )
                fk_map[fk_name]["referred_columns"].append(
                    (
                        int(row._mapping["key_sequence"]),
                        self.normalize_name(row._mapping["pk_column_name"]),
                    )
                )
        result = defaultdict(list)
        for fk_info in fk_map.values():
            fk_table_name = fk_info.pop("_fk_table_name")
            fk_info["constrained_columns"] = [
                col for _, col in sorted(fk_info["constrained_columns"])
            ]
            fk_info["referred_columns"] = [
                col for _, col in sorted(fk_info["referred_columns"])
            ]
            result[fk_table_name].append(fk_info)
        return dict(result)

    # ---------------------------------------------------------------------------
    # Primary key reflection
    # ---------------------------------------------------------------------------

    def _get_table_primary_keys(self, connection, table_name, schema, **kw):
        """SHOW PRIMARY KEYS IN TABLE — single-table path (cache_column_metadata=True).

        Results are cached by the calling method's @reflection.cache decorator.
        """
        full_name = self._always_quote_join(schema, table_name)
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_primary_keys */ PRIMARY KEYS IN TABLE {full_name}"
                )
            )
            normalized_table_name = self.normalize_name(table_name)
            return self._parse_pk_rows(result).get(
                normalized_table_name, {"constrained_columns": [], "name": None}
            )
        except sa_exc.ProgrammingError:
            logger.debug("Failed to reflect primary keys for %s", full_name)
            return {"constrained_columns": [], "name": None}

    @reflection.cache
    def _get_schema_primary_keys(self, connection, schema, **kw):
        """SHOW PRIMARY KEYS IN SCHEMA — schema-wide path for get_pk_constraint
        (SA 1.4) and get_multi_pk_constraint (SA 2.x).

        Results are cached for the lifetime of a connection via @reflection.cache.
        DDL executed mid-session will not be reflected until a new connection is used.
        """
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:_get_schema_primary_keys */ PRIMARY KEYS IN SCHEMA {schema}"
            )
        )
        return self._parse_pk_rows(result)

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        if self._is_single_table_reflection(schema, **kw):
            return self._get_table_primary_keys(connection, table_name, schema, **kw)
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        return self._get_schema_primary_keys(
            connection, self.denormalize_name(full_schema_name), **kw
        ).get(table_name, {"constrained_columns": [], "name": None})

    def get_multi_pk_constraint(
        self,
        connection,
        *,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        **kw,
    ):
        """SA 2.x bulk hook — called during MetaData.reflect() instead of
        get_pk_constraint.  Always uses SHOW PRIMARY KEYS IN SCHEMA so the
        result is fetched once and cached for the full reflection pass.

        The return key uses the original ``schema`` value (possibly None) so
        SA's _reflect_info lookup succeeds when schema was not explicitly set.
        """
        effective_schema = schema or self.default_schema_name
        full_schema_name = self._get_full_schema_name(
            connection, effective_schema, **kw
        )
        all_pks = self._get_schema_primary_keys(
            connection, self.denormalize_name(full_schema_name), **kw
        )
        tables = filter_names if filter_names is not None else list(all_pks.keys())
        return [
            (
                (schema, table_name),
                all_pks.get(table_name, {"constrained_columns": [], "name": None}),
            )
            for table_name in tables
        ]

    # ---------------------------------------------------------------------------
    # Unique constraint reflection
    # ---------------------------------------------------------------------------

    def _get_table_unique_constraints(self, connection, table_name, schema, **kw):
        """SHOW UNIQUE KEYS IN TABLE — single-table path (cache_column_metadata=True).

        Results are cached by the calling method's @reflection.cache decorator.
        """
        full_name = self._always_quote_join(schema, table_name)
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_unique_constraints */ UNIQUE KEYS IN TABLE {full_name}"
                )
            )
            normalized_table_name = self.normalize_name(table_name)
            return self._parse_uk_rows(result).get(normalized_table_name, [])
        except sa_exc.ProgrammingError:
            logger.debug("Failed to reflect unique constraints for %s", full_name)
            return []

    @reflection.cache
    def _get_schema_unique_constraints(self, connection, schema, **kw):
        """SHOW UNIQUE KEYS IN SCHEMA — schema-wide path for get_unique_constraints
        (SA 1.4) and get_multi_unique_constraints (SA 2.x).

        Results are cached for the lifetime of a connection via @reflection.cache.
        DDL executed mid-session will not be reflected until a new connection is used.
        """
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:_get_schema_unique_constraints */ UNIQUE KEYS IN SCHEMA {schema}"
            )
        )
        return self._parse_uk_rows(result)

    def get_unique_constraints(self, connection, table_name, schema, **kw):
        schema = schema or self.default_schema_name
        if self._is_single_table_reflection(schema, **kw):
            return self._get_table_unique_constraints(
                connection, table_name, schema, **kw
            )
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        return self._get_schema_unique_constraints(
            connection, self.denormalize_name(full_schema_name), **kw
        ).get(table_name, [])

    def get_multi_unique_constraints(
        self,
        connection,
        *,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        **kw,
    ):
        """SA 2.x bulk hook — called during MetaData.reflect() instead of
        get_unique_constraints.  Always uses SHOW UNIQUE KEYS IN SCHEMA.

        The return key uses the original ``schema`` value (possibly None) so
        SA's _reflect_info lookup succeeds when schema was not explicitly set.
        """
        effective_schema = schema or self.default_schema_name
        full_schema_name = self._get_full_schema_name(
            connection, effective_schema, **kw
        )
        all_uk = self._get_schema_unique_constraints(
            connection, self.denormalize_name(full_schema_name), **kw
        )
        tables = filter_names if filter_names is not None else list(all_uk.keys())
        return [
            ((schema, table_name), all_uk.get(table_name, [])) for table_name in tables
        ]

    # ---------------------------------------------------------------------------
    # Foreign key reflection
    # ---------------------------------------------------------------------------

    def _get_table_foreign_keys(self, connection, table_name, schema, **kw):
        """SHOW IMPORTED KEYS IN TABLE — single-table path (cache_column_metadata=True).

        referred_schema is set to None when the FK target is in the same schema as
        the table being reflected.  The same-schema set always includes the
        explicitly-reflected schema so that cross-session-schema scenarios
        (e.g. USE SCHEMA called after engine creation, or reflecting a non-default
        schema) are handled correctly.

        Results are cached by the calling method's @reflection.cache decorator.
        """
        full_name = self._always_quote_join(schema, table_name)
        _, current_schema = self._current_database_schema(connection, **kw)
        same_schemas = {
            self.normalize_name(schema),
            self.default_schema_name,
            current_schema,
        }
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_foreign_keys */ IMPORTED KEYS IN TABLE {full_name}"
                )
            )
            normalized_table_name = self.normalize_name(table_name)
            return self._parse_fk_rows(result, same_schemas).get(
                normalized_table_name, []
            )
        except sa_exc.ProgrammingError:
            logger.debug("Failed to reflect foreign keys for %s", full_name)
            return []

    @reflection.cache
    def _get_schema_foreign_keys(self, connection, schema, **kw):
        """SHOW IMPORTED KEYS IN SCHEMA — schema-wide path for get_foreign_keys
        (SA 1.4) and get_multi_foreign_keys (SA 2.x).

        referred_schema is set to None for FKs whose target is in the same
        schema as the table being reflected.  The same-schema set includes the
        explicitly-reflected schema so that cross-session-schema scenarios
        (e.g. USE SCHEMA called after engine creation, or reflecting a
        non-default schema) are handled correctly.

        See:
        https://docs.sqlalchemy.org/en/14/core/reflection.html#reflection-schema-qualified-interaction

        Results are cached for the lifetime of a connection via @reflection.cache.
        DDL executed mid-session will not be reflected until a new connection is used.
        """
        _, current_schema = self._current_database_schema(connection, **kw)
        same_schemas = {
            self.normalize_name(schema),
            self.default_schema_name,
            current_schema,
        }
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:_get_schema_foreign_keys */ IMPORTED KEYS IN SCHEMA {schema}"
            )
        )
        return self._parse_fk_rows(result, same_schemas)

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Gets all foreign keys for a table."""
        schema = schema or self.default_schema_name
        if self._is_single_table_reflection(schema, **kw):
            return self._get_table_foreign_keys(connection, table_name, schema, **kw)
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        return self._get_schema_foreign_keys(
            connection, self.denormalize_name(full_schema_name), **kw
        ).get(table_name, [])

    def get_multi_foreign_keys(
        self,
        connection,
        *,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        **kw,
    ):
        """SA 2.x bulk hook — called during MetaData.reflect() instead of
        get_foreign_keys.  Always uses SHOW IMPORTED KEYS IN SCHEMA.

        The return key uses the original ``schema`` value (possibly None) so
        SA's _reflect_info lookup succeeds when schema was not explicitly set.
        """
        effective_schema = schema or self.default_schema_name
        full_schema_name = self._get_full_schema_name(
            connection, effective_schema, **kw
        )
        all_fks = self._get_schema_foreign_keys(
            connection, self.denormalize_name(full_schema_name), **kw
        )
        tables = filter_names if filter_names is not None else list(all_fks.keys())
        return [
            ((schema, table_name), all_fks.get(table_name, [])) for table_name in tables
        ]

    def get_multi_columns(
        self,
        connection,
        *,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        **kw,
    ):
        """SA 2.x bulk hook — called by reflect_table/_get_reflection_info for
        every reflection operation (single table and MetaData.reflect() alike).

        Uses the cached schema-wide information_schema query when available.
        Falls back to DESC TABLE per-table for objects not in information_schema
        (temp tables, dynamic tables, etc.).

        Important: the return key must use the original ``schema`` value (which
        may be None), not a normalised substitute, so SA's _reflect_info lookup
        succeeds when schema was not explicitly provided.
        """
        # effective_schema drives the SQL query; original schema drives the key.
        effective_schema = schema or self.default_schema_name
        if not effective_schema:
            _, effective_schema = self._current_database_schema(connection, **kw)
        all_columns = self._get_schema_columns(connection, effective_schema, **kw)
        if all_columns is None:
            all_columns = {}
        tables = filter_names if filter_names is not None else list(all_columns.keys())
        mgr = _StructuredTypeInfoManager(
            connection, self.name_utils, self.default_schema_name
        )
        result = []
        for table_name in tables:
            cols = all_columns.get(table_name)
            if cols is None:
                full_name = self._always_quote_join(effective_schema, table_name)
                cols = mgr.get_table_columns_by_full_name(full_name)
            result.append(((schema, table_name), cols))
        return result

    def _get_type_kwargs(
        self, col_type, character_maximum_length, numeric_precision, numeric_scale
    ):
        """
        Build type constructor kwargs based on SQLAlchemy type class.

        Args:
            col_type: SQLAlchemy type class
            character_maximum_length: Max length for string/binary types
            numeric_precision: Precision for numeric types
            numeric_scale: Scale for numeric types

        Returns:
            Dictionary of kwargs for type constructor
        """
        if issubclass(col_type, FLOAT):
            return {
                "precision": numeric_precision,
                "decimal_return_scale": numeric_scale,
            }
        elif issubclass(col_type, sqltypes.Numeric):
            return {"precision": numeric_precision, "scale": numeric_scale}
        elif issubclass(col_type, (sqltypes.String, sqltypes.BINARY)):
            return {"length": character_maximum_length}
        return {}

    def _resolve_column_type(
        self,
        coltype,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        data_type_alias,
        column_name,
    ):
        """
        Resolve SQLAlchemy type from Snowflake type name.

        Args:
            coltype: Snowflake type name (e.g., "NUMBER", "VARCHAR")
            character_maximum_length: Max length for string/binary types
            numeric_precision: Precision for numeric types
            numeric_scale: Scale for numeric types
            data_type_alias: Original type name with parameters (for VECTOR)
            column_name: Column name for warning messages

        Returns:
            SQLAlchemy type instance

        Special handling:
            - VECTOR: Parsed from data_type_alias text
            - Unknown types: Returns NullType with warning
        """
        col_type = self.ischema_names.get(coltype)

        if col_type is None:
            sa_util.warn(
                f"Did not recognize type '{coltype}' of column '{column_name}'"
            )
            return NullType()

        if issubclass(col_type, VECTOR):
            return parse_type(data_type_alias)

        type_kwargs = self._get_type_kwargs(
            col_type, character_maximum_length, numeric_precision, numeric_scale
        )
        return col_type(**type_kwargs)

    def _build_identity_metadata(
        self,
        identity_start,
        identity_increment,
        identity_generation,
        identity_cycle,
        identity_ordered,
    ):
        """
        Build SQLAlchemy 2.0 ReflectedIdentity metadata for identity columns.

        Args:
            identity_start: START value from IDENTITY definition
            identity_increment: INCREMENT value from IDENTITY definition
            identity_generation: "BY DEFAULT" (Snowflake only supports this)
            identity_cycle: "NO" (Snowflake only supports NO CYCLE)
            identity_ordered: "YES" or "NO" for ORDER/NOORDER property

        Returns:
            Dictionary conforming to SQLAlchemy ReflectedIdentity TypedDict

        Snowflake constraints:
            - Only supports BY DEFAULT (not GENERATED ALWAYS)
            - Only supports NO CYCLE
            - Does not separately track ON NULL behavior
            - Does not expose min/max/cache in information_schema

        See: https://docs.sqlalchemy.org/en/20/core/reflection.html
        """
        return {
            "start": identity_start,
            "increment": identity_increment,
            "always": False,
            "on_null": None,
            "cycle": False,
            "order": identity_ordered == "YES" if identity_ordered else None,
            "minvalue": None,
            "maxvalue": None,
            "nominvalue": None,
            "nomaxvalue": None,
            "cache": None,
        }

    def _is_primary_key_column(self, table_name, column_name, schema_primary_keys):
        """
        Check if a column is part of the primary key.

        Args:
            table_name: Normalized table name
            column_name: Normalized column name
            schema_primary_keys: Dict mapping table names to PK constraint info

        Returns:
            True if column is in the primary key, False otherwise
        """
        pk_info = schema_primary_keys.get(table_name)
        if not pk_info:
            return False
        return column_name in pk_info["constrained_columns"]

    def _build_column_info(
        self,
        table_name,
        column_name,
        coltype,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        is_nullable,
        column_default,
        is_identity,
        comment,
        identity_start,
        identity_increment,
        identity_generation,
        identity_cycle,
        identity_ordered,
        data_type_alias,
        schema_name,
        schema_primary_keys,
        structured_type_info_manager,
        **kw,
    ):
        """
        Build column metadata dictionary from query row.

        Args:
            Various column attributes from information_schema.columns
            schema_name: Denormalized schema name for structured type lookup
            schema_primary_keys: PK constraint info for the schema
            structured_type_info_manager: Manager for structured type introspection

        Returns:
            Tuple of (table_name, column_info), or None if column should be ignored
        """
        table_name = self.normalize_name(table_name)
        column_name = self.normalize_name(column_name)

        if column_name.startswith("sys_clustering_column"):
            return None

        # Try structured type handling first if the type is recognized as structured
        col_type = self.ischema_names.get(coltype)
        if col_type is not None and issubclass(col_type, StructuredType):
            column_info = structured_type_info_manager.get_column_info(
                schema_name, table_name, column_name, **kw
            )
            if column_info:
                return table_name, column_info
            # If structured type info not available, fall through to normal type handling

        type_instance = self._resolve_column_type(
            coltype,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            data_type_alias,
            column_name,
        )

        column_info = {
            "name": column_name,
            "type": type_instance,
            "nullable": is_nullable == "YES",
            "default": column_default,
            "autoincrement": is_identity == "YES",
            "comment": comment,
            "primary_key": self._is_primary_key_column(
                table_name, column_name, schema_primary_keys
            ),
        }

        if is_identity == "YES":
            column_info["identity"] = self._build_identity_metadata(
                identity_start,
                identity_increment,
                identity_generation,
                identity_cycle,
                identity_ordered,
            )

        return table_name, column_info

    @reflection.cache
    def _get_schema_columns(self, connection, schema, **kw):
        """
        Get all columns in the schema with complete metadata.

        Args:
            connection: Database connection
            schema: Schema name to reflect
            **kw: Additional arguments including optional info_cache

        Returns:
            Dictionary mapping table names to lists of column info dicts,
            or None if information_schema query returned too much data

        Note:
            Returns None (cacheable) when hitting Snowflake's information_schema
            result size limit, triggering fallback to per-table DESC queries.
        """
        schema_name = self.denormalize_name(schema)

        result = self._query_all_columns_info(connection, schema_name, **kw)
        if result is None:
            return None

        current_database, default_schema = self._current_database_schema(
            connection, **kw
        )
        full_schema_name = self._denormalize_quote_join(current_database, schema)

        schema_primary_keys = self._get_schema_primary_keys(
            connection, full_schema_name, **kw
        )

        structured_type_info_manager = _StructuredTypeInfoManager(
            connection, self.name_utils, default_schema
        )

        columns_by_table = {}

        for (
            table_name,
            column_name,
            coltype,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            is_identity,
            comment,
            identity_start,
            identity_increment,
            identity_generation,
            identity_cycle,
            identity_ordered,
            data_type_alias,
        ) in result:
            column_result = self._build_column_info(
                table_name,
                column_name,
                coltype,
                character_maximum_length,
                numeric_precision,
                numeric_scale,
                is_nullable,
                column_default,
                is_identity,
                comment,
                identity_start,
                identity_increment,
                identity_generation,
                identity_cycle,
                identity_ordered,
                data_type_alias,
                schema_name,
                schema_primary_keys,
                structured_type_info_manager,
                **kw,
            )

            if column_result is None:
                continue

            normalized_table_name, column_info = column_result
            columns_by_table.setdefault(normalized_table_name, []).append(column_info)

        return columns_by_table

    def _is_single_table_reflection(self, schema, **kw):
        """Return True when a single-table SHOW command should be used for PK/UK/FK.

        Opt-in via ``cache_column_metadata=true`` on the engine URL or
        connect_args.  When disabled (the default) all reflection uses the
        existing schema-wide queries unchanged, preserving backward compatibility.

        SA 2.x path (IS_VERSION_20=True):
          ``get_multi_pk_constraint`` / ``get_multi_unique_constraints`` /
          ``get_multi_foreign_keys`` are used by MetaData.reflect(), so any call
          to the singular get_pk_constraint / get_unique_constraints /
          get_foreign_keys is inherently single-table (Inspector,
          pandas.read_sql_table).  No info_cache inspection required.

        SA 1.4 path (IS_VERSION_20=False):
          MetaData.reflect() calls the singular methods per-table and populates
          ``_get_schema_tables_info`` early in the reflection pass.  The
          presence of that key in info_cache is the signal that multi-table
          reflection is in progress; fall back to the schema-wide cached query.

        Note: @reflection.cache caches results for the lifetime of a connection.
        DDL executed mid-session will not be visible in reflection until a new
        connection is obtained, regardless of which path is used.
        """
        if not getattr(self, "_cache_column_metadata", False):
            return False

        if IS_VERSION_20:
            # SA 2.x: get_multi_* handles MetaData.reflect(); singular calls
            # are always single-table at this point.
            return True

        # SA 1.4: detect whether MetaData.reflect() is in progress.
        # @reflection.cache stores keys as (fn.__name__, args_tuple, kwargs_tuple).
        info_cache = kw.get("info_cache")
        if info_cache is None:
            return True
        tables_info_key = (self._get_schema_tables_info.__name__, (schema,), ())
        return tables_info_key not in info_cache

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Gets all column info given the table info
        """
        schema = schema or self.default_schema_name
        if not schema:
            _, schema = self._current_database_schema(connection, **kw)

        # SA 2.x: get_multi_columns handles MetaData.reflect(); get_columns is
        # always a single-table call here.  Use DESC TABLE — it works for all
        # table types including temp tables not visible in information_schema.
        # SA 1.4: opt-in via cache_column_metadata=True (_is_single_table_reflection
        # returns True only when single-table); otherwise falls through to the
        # schema-wide path used by MetaData.reflect() per-table loops.
        if (
            IS_VERSION_20 or self._is_single_table_reflection(schema, **kw)
        ) and table_name:
            full_table_name = self._always_quote_join(schema, table_name)
            column_info_manager = _StructuredTypeInfoManager(
                connection, self.name_utils, self.default_schema_name
            )
            return column_info_manager.get_table_columns_by_full_name(full_table_name)

        # Use schema-wide cached query (optimal for multi-table reflection)
        schema_columns = self._get_schema_columns(connection, schema, **kw)
        if schema_columns is None:
            column_info_manager = _StructuredTypeInfoManager(
                connection, self.name_utils, self.default_schema_name
            )
            # Too many results, fall back to only query about single table
            return column_info_manager.get_table_columns(table_name, schema)
        normalized_table_name = self.normalize_name(table_name)
        if normalized_table_name not in schema_columns:
            raise sa_exc.NoSuchTableError()
        return schema_columns[normalized_table_name]

    def get_prefixes_from_data(self, name_to_index_map, row, **kw):
        prefixes_found = []
        for valid_prefix in CustomTablePrefix:
            key = f"is_{valid_prefix.name.lower()}"
            if key in name_to_index_map and row[name_to_index_map[key]] == "Y":
                prefixes_found.append(valid_prefix.name)
        return prefixes_found

    @reflection.cache
    def _query_all_columns_info(self, connection, schema_name, **kw):
        try:
            return connection.execute(
                text(
                    """
            SELECT /* sqlalchemy:_get_schema_columns */
                   ic.table_name,
                   ic.column_name,
                   ic.data_type,
                   ic.character_maximum_length,
                   ic.numeric_precision,
                   ic.numeric_scale,
                   ic.is_nullable,
                   ic.column_default,
                   ic.is_identity,
                   ic.comment,
                   ic.identity_start,
                   ic.identity_increment,
                   ic.identity_generation,
                   ic.identity_cycle,
                   ic.identity_ordered,
                   ic.data_type_alias
              FROM information_schema.columns ic
             WHERE ic.table_schema=:table_schema
             ORDER BY ic.ordinal_position"""
                ),
                {"table_schema": schema_name},
            )
        except sa_exc.ProgrammingError as pe:
            if pe.orig.errno == 90030:
                # This means that there are too many tables in the schema, we need to go more granular
                return None  # None triggers get_table_columns while staying cacheable
            raise

    @reflection.cache
    def _get_schema_tables_info(self, connection, schema=None, **kw):
        """
        Retrieves information about all tables in the specified schema.
        """

        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:get_schema_tables_info */ TABLES IN SCHEMA {full_schema_name}"
            )
        )

        name_to_index_map = self._map_name_to_idx(result)
        tables = {}
        for row in result.cursor.fetchall():
            table_name = self.normalize_name(str(row[name_to_index_map["name"]]))
            table_prefixes = self.get_prefixes_from_data(name_to_index_map, row)
            tables[table_name] = {"prefixes": table_prefixes}

        return tables

    def get_table_names(self, connection, schema=None, **kw):
        """
        Gets all table names.
        """
        ret = self._get_schema_tables_info(
            connection, schema, info_cache=kw.get("info_cache", None)
        ).keys()
        return list(ret)

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        Gets all view names
        """
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        cursor = connection.execute(
            text(f"SHOW /* sqlalchemy:get_view_names */ VIEWS IN {full_schema_name}")
        )

        return [self.normalize_name(row[1]) for row in cursor]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """
        Gets the view definition
        """
        schema = schema or self.default_schema_name
        if schema:
            cursor = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:get_view_definition */ VIEWS \
                    LIKE '{self._denormalize_quote_join(view_name)}' IN {self._denormalize_quote_join(schema)}"
                )
            )
        else:
            cursor = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:get_view_definition */ VIEWS \
                    LIKE '{self._denormalize_quote_join(view_name)}'"
                )
            )

        name_to_index_map = self.__class__._map_name_to_idx(cursor)
        try:
            ret = cursor.fetchone()
            if ret:
                return ret[name_to_index_map["text"]]
        except Exception:
            pass
        return None

    def get_temp_table_names(self, connection, schema=None, **kw):
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        cursor = connection.execute(
            text(
                f"SHOW /* sqlalchemy:get_temp_table_names */ TABLES IN SCHEMA {full_schema_name}"
            )
        )

        ret = []
        name_to_index_map = self.__class__._map_name_to_idx(cursor)
        for row in cursor:
            if row[name_to_index_map["kind"]] == "TEMPORARY":
                ret.append(self.normalize_name(row[name_to_index_map["name"]]))

        return ret

    def get_schema_names(self, connection, **kw):
        """
        Gets all schema names.
        """
        cursor = connection.execute(
            text("SHOW /* sqlalchemy:get_schema_names */ SCHEMAS")
        )

        return [self.normalize_name(row[1]) for row in cursor]

    @reflection.cache
    def get_sequence_names(self, connection, schema=None, **kw):
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        sql_command = f"SHOW SEQUENCES IN SCHEMA {full_schema_name}"
        try:
            cursor = connection.execute(text(sql_command))
            return [self.normalize_name(row[0]) for row in cursor]
        except sa_exc.ProgrammingError as pe:
            if pe.orig.errno == 2003:
                # Schema does not exist
                return []

    def _get_table_comment(self, connection, table_name, schema=None, **kw):
        """
        Returns comment of table in a dictionary as described by SQLAlchemy spec.
        """
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        sql_command = (
            "SHOW /* sqlalchemy:_get_table_comment */ "
            f"TABLES LIKE '{table_name}' IN SCHEMA {full_schema_name}"
        )
        cursor = connection.execute(text(sql_command))
        return cursor.fetchone()

    def _get_view_comment(self, connection, table_name, schema=None, **kw):
        """
        Returns comment of view in a dictionary as described by SQLAlchemy spec.
        """
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        sql_command = (
            "SHOW /* sqlalchemy:_get_view_comment */ "
            f"VIEWS LIKE '{table_name}' IN SCHEMA {full_schema_name}"
        )
        cursor = connection.execute(text(sql_command))
        return cursor.fetchone()

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        """
        Returns comment associated with a table (or view) in a dictionary as
        SQLAlchemy expects. Note that since SQLAlchemy may not (in fact,
        typically does not) know if this is a table or a view, we have to
        handle both cases here.
        """
        result = self._get_table_comment(connection, table_name, schema, **kw)
        if result is None:
            # the "table" being reflected is actually a view
            result = self._get_view_comment(connection, table_name, schema, **kw)

        return {
            "text": (
                result._mapping["comment"]
                if result and result._mapping["comment"]
                else None
            )
        }

    def get_table_names_with_prefix(
        self,
        connection,
        *,
        schema,
        prefix,
        **kw,
    ):
        tables_data = self._get_schema_tables_info(connection, schema, **kw)
        table_names = []
        for table_name, tables_data_value in tables_data.items():
            if prefix in tables_data_value["prefixes"]:
                table_names.append(table_name)
        return table_names

    def get_multi_indexes(
        self,
        connection,
        *,
        schema: Optional[str] = None,
        filter_names: Optional[Collection[str]] = None,
        **kw,
    ):
        """
        Gets the indexes definition
        """
        schema = schema or self.default_schema_name
        hybrid_table_names = self.get_table_names_with_prefix(
            connection,
            schema=schema,
            prefix=CustomTablePrefix.HYBRID.name,
            info_cache=kw.get("info_cache", None),
        )
        if len(hybrid_table_names) == 0:
            return []

        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:get_multi_indexes */ INDEXES IN SCHEMA {full_schema_name}"
            )
        )

        all_indexes = self._parse_index_rows(result)
        return [
            ((schema, table_name), table_indexes)
            for table_name, table_indexes in all_indexes.items()
            if table_name in filter_names and table_name in hybrid_table_names
        ]

    def _value_or_default(self, data, table, schema):
        table = self.normalize_name(str(table))
        dic_data = dict(data)
        if (schema, table) in dic_data:
            return dic_data[(schema, table)]
        else:
            return []

    def _parse_index_rows(self, result):
        """Parse SHOW INDEXES rows into {table_name: [index_dict, ...]}.

        Both SHOW INDEXES IN TABLE and SHOW INDEXES IN SCHEMA return the same
        column set (including table), so this helper works for both paths.
        SYS_INDEX primary-key sentinels are filtered out.
        """
        name_to_index_map = self._map_name_to_idx(result)
        indexes = defaultdict(list)
        for row in result.cursor.fetchall():
            if (
                row[name_to_index_map["name"]]
                == f'SYS_INDEX_{row[name_to_index_map["table"]]}_PRIMARY'
            ):
                continue
            table_name = self.normalize_name(str(row[name_to_index_map["table"]]))
            indexes[table_name].append(
                {
                    "name": row[name_to_index_map["name"]],
                    "unique": row[name_to_index_map["is_unique"]] == "Y",
                    "column_names": [
                        self.normalize_name(column)
                        for column in parse_index_columns(
                            row[name_to_index_map["columns"]]
                        )
                    ],
                    "include_columns": [
                        self.normalize_name(column)
                        for column in parse_index_columns(
                            row[name_to_index_map["included_columns"]]
                        )
                    ],
                    "dialect_options": {},
                }
            )
        return dict(indexes)

    def _get_table_indexes(self, connection, table_name, schema, **kw):
        """SHOW INDEXES IN TABLE — single-table path (cache_column_metadata=True).

        For non-hybrid tables Snowflake returns an empty result set (not an
        error), so the list will simply be empty.  The SYS_INDEX primary-key
        sentinel is filtered out, consistent with the schema-wide path.

        Results are cached by the calling method's @reflection.cache decorator.
        """
        full_name = self._always_quote_join(schema, table_name)
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_indexes */ INDEXES IN TABLE {full_name}"
                )
            )
            normalized_table_name = self.normalize_name(table_name)
            return self._parse_index_rows(result).get(normalized_table_name, [])
        except sa_exc.ProgrammingError:
            logger.debug("Failed to reflect indexes for %s", full_name)
            return []

    @reflection.cache
    def get_indexes(self, connection, tablename, schema, **kw):
        """Gets the indexes definition."""
        schema = schema or self.default_schema_name
        table_name = self.normalize_name(str(tablename))
        if self._is_single_table_reflection(schema, **kw):
            return self._get_table_indexes(connection, table_name, schema, **kw)
        data = self.get_multi_indexes(
            connection=connection, schema=schema, filter_names=[table_name], **kw
        )
        return self._value_or_default(data, table_name, schema)

    def connect(self, *cargs, **cparams):
        if _ENABLE_SQLALCHEMY_AS_APPLICATION_NAME:
            cparams = _update_connection_application_name(**cparams)

        # Set decimal precision for full DECFLOAT support (38 digits)
        if self._enable_decfloat:
            decimal.getcontext().prec = DECFLOAT_PRECISION

        connection = super().connect(*cargs, **cparams)
        self._log_new_connection_event(connection)

        return connection

    def _log_new_connection_event(self, connection):
        try:
            snowflake_connection = cast(SnowflakeConnection, cast(object, connection))
            snowflake_telemetry_client = TelemetryClient(rest=snowflake_connection.rest)

            telemetry_value = {
                "SQLAlchemy": SQLALCHEMY_VERSION,
            }
            try:
                from pandas import __version__ as PANDAS_VERSION

                telemetry_value["pandas"] = PANDAS_VERSION
            except ImportError:
                pass

            snowflake_telemetry_client.add_log_to_batch(
                TelemetryData.from_telemetry_data_dict(
                    from_dict={
                        TelemetryField.KEY_TYPE.value: TelemetryEvents.NEW_CONNECTION.value,
                        TelemetryField.KEY_VALUE.value: str(telemetry_value),
                    },
                    timestamp=int(time_in_seconds() * 1000),
                    connection=snowflake_connection,
                )
            )
            snowflake_telemetry_client.send_batch()
        except Exception as e:
            logger.debug(
                "Failed to send telemetry data for %s event: %s: %s",
                TelemetryEvents.NEW_CONNECTION.value,
                type(e).__name__,
                str(e),
            )


@sa_vnt.listens_for(Table, "before_create")
def check_table(table, connection, _ddl_runner, **kw):
    from .sql.custom_schema.hybrid_table import HybridTable

    if HybridTable.is_equal_type(table):  # noqa
        return True
    if isinstance(_ddl_runner.dialect, SnowflakeDialect) and table.indexes:
        raise NotImplementedError("Only Snowflake Hybrid Tables supports indexes")


dialect = SnowflakeDialect
