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
from snowflake.sqlalchemy.compat import returns_unicode
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
        **kwargs: Any,
    ):
        super().__init__(isolation_level=isolation_level, **kwargs)
        self.force_div_is_floordiv = force_div_is_floordiv
        self.div_is_floordiv = force_div_is_floordiv
        self.name_utils = _NameUtils(self.identifier_preparer)
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

    def _get_table_primary_keys(self, connection, table_name, schema, **kw):
        """
        Get primary key constraint for a specific table using table-specific query.
        Used when cache_column_metadata=False for optimized single-table reflection.
        """
        full_name = self._denormalize_quote_join(schema, table_name)
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_primary_keys */ PRIMARY KEYS IN TABLE {full_name}"
                )
            )
            constrained_columns = []
            constraint_name = None
            for row in result:
                if constraint_name is None:
                    constraint_name = self.normalize_name(
                        row._mapping["constraint_name"]
                    )
                constrained_columns.append(
                    self.normalize_name(row._mapping["column_name"])
                )
            if constraint_name:
                return {
                    "constrained_columns": constrained_columns,
                    "name": constraint_name,
                }
            return {"constrained_columns": [], "name": None}
        except sa_exc.DBAPIError:
            return {"constrained_columns": [], "name": None}

    @reflection.cache
    def _get_schema_primary_keys(self, connection, schema, **kw):
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:_get_schema_primary_keys */PRIMARY KEYS IN SCHEMA {schema}"
            )
        )
        ans = {}
        for row in result:
            table_name = self.normalize_name(row._mapping["table_name"])
            if table_name not in ans:
                ans[table_name] = {
                    "constrained_columns": [],
                    "name": self.normalize_name(row._mapping["constraint_name"]),
                }
            ans[table_name]["constrained_columns"].append(
                self.normalize_name(row._mapping["column_name"])
            )
        return ans

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name

        # Use smart detection to decide between table-specific vs schema-wide query
        if self._should_use_table_specific_query(schema, **kw):
            return self._get_table_primary_keys(connection, table_name, schema, **kw)

        # Use schema-wide cached query (optimal for full schema reflection)
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        return self._get_schema_primary_keys(
            connection, self.denormalize_name(full_schema_name), **kw
        ).get(table_name, {"constrained_columns": [], "name": None})

    def _get_table_unique_constraints(self, connection, table_name, schema, **kw):
        """
        Get unique constraints for a specific table using table-specific query.
        Used when cache_column_metadata=False for optimized single-table reflection.
        """
        full_name = self._denormalize_quote_join(schema, table_name)
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_unique_constraints */ UNIQUE KEYS IN TABLE {full_name}"
                )
            )
            unique_constraints = {}
            for row in result:
                name = self.normalize_name(row._mapping["constraint_name"])
                if name not in unique_constraints:
                    unique_constraints[name] = {
                        "column_names": [
                            self.normalize_name(row._mapping["column_name"])
                        ],
                        "name": name,
                    }
                else:
                    unique_constraints[name]["column_names"].append(
                        self.normalize_name(row._mapping["column_name"])
                    )
            return list(unique_constraints.values())
        except sa_exc.DBAPIError:
            return []

    @reflection.cache
    def _get_schema_unique_constraints(self, connection, schema, **kw):
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:_get_schema_unique_constraints */ UNIQUE KEYS IN SCHEMA {schema}"
            )
        )
        unique_constraints = {}
        for row in result:
            name = self.normalize_name(row._mapping["constraint_name"])
            if name not in unique_constraints:
                unique_constraints[name] = {
                    "column_names": [self.normalize_name(row._mapping["column_name"])],
                    "name": name,
                    "table_name": self.normalize_name(row._mapping["table_name"]),
                }
            else:
                unique_constraints[name]["column_names"].append(
                    self.normalize_name(row._mapping["column_name"])
                )

        ans = defaultdict(list)
        for constraint in unique_constraints.values():
            table_name = constraint.pop("table_name")
            ans[table_name].append(constraint)
        return ans

    def get_unique_constraints(self, connection, table_name, schema, **kw):
        schema = schema or self.default_schema_name

        # Use smart detection to decide between table-specific vs schema-wide query
        if self._should_use_table_specific_query(schema, **kw):
            return self._get_table_unique_constraints(
                connection, table_name, schema, **kw
            )

        # Use schema-wide cached query (optimal for full schema reflection)
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        return self._get_schema_unique_constraints(
            connection, self.denormalize_name(full_schema_name), **kw
        ).get(table_name, [])

    def _get_table_foreign_keys(self, connection, table_name, schema, **kw):
        """
        Get foreign keys for a specific table using table-specific query.
        Used when cache_column_metadata=False or in single-table reflection.
        """
        full_name = self._denormalize_quote_join(schema, table_name)
        _, current_schema = self._current_database_schema(connection, **kw)
        try:
            result = connection.execute(
                text(
                    f"SHOW /* sqlalchemy:_get_table_foreign_keys */ IMPORTED KEYS IN TABLE {full_name}"
                )
            )
            foreign_key_map = {}
            for row in result:
                name = self.normalize_name(row._mapping["fk_name"])
                if name not in foreign_key_map:
                    referred_schema = self.normalize_name(
                        row._mapping["pk_schema_name"]
                    )
                    foreign_key_map[name] = {
                        "constrained_columns": [
                            self.normalize_name(row._mapping["fk_column_name"])
                        ],
                        "referred_schema": (
                            referred_schema
                            if referred_schema
                            not in (self.default_schema_name, current_schema)
                            else None
                        ),
                        "referred_table": self.normalize_name(
                            row._mapping["pk_table_name"]
                        ),
                        "referred_columns": [
                            self.normalize_name(row._mapping["pk_column_name"])
                        ],
                        "name": name,
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
                    foreign_key_map[name]["options"] = options
                else:
                    foreign_key_map[name]["constrained_columns"].append(
                        self.normalize_name(row._mapping["fk_column_name"])
                    )
                    foreign_key_map[name]["referred_columns"].append(
                        self.normalize_name(row._mapping["pk_column_name"])
                    )
            return list(foreign_key_map.values())
        except sa_exc.DBAPIError:
            return []

    @reflection.cache
    def _get_schema_foreign_keys(self, connection, schema, **kw):
        _, current_schema = self._current_database_schema(connection, **kw)
        result = connection.execute(
            text(
                f"SHOW /* sqlalchemy:_get_schema_foreign_keys */ IMPORTED KEYS IN SCHEMA {schema}"
            )
        )
        foreign_key_map = {}
        for row in result:
            name = self.normalize_name(row._mapping["fk_name"])
            if name not in foreign_key_map:
                referred_schema = self.normalize_name(row._mapping["pk_schema_name"])
                foreign_key_map[name] = {
                    "constrained_columns": [
                        self.normalize_name(row._mapping["fk_column_name"])
                    ],
                    # referred schema should be None in context where it doesn't need to be specified
                    # https://docs.sqlalchemy.org/en/14/core/reflection.html#reflection-schema-qualified-interaction
                    "referred_schema": (
                        referred_schema
                        if referred_schema
                        not in (self.default_schema_name, current_schema)
                        else None
                    ),
                    "referred_table": self.normalize_name(
                        row._mapping["pk_table_name"]
                    ),
                    "referred_columns": [
                        self.normalize_name(row._mapping["pk_column_name"])
                    ],
                    "name": name,
                    "table_name": self.normalize_name(row._mapping["fk_table_name"]),
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
                foreign_key_map[name]["options"] = options
            else:
                foreign_key_map[name]["constrained_columns"].append(
                    self.normalize_name(row._mapping["fk_column_name"])
                )
                foreign_key_map[name]["referred_columns"].append(
                    self.normalize_name(row._mapping["pk_column_name"])
                )

        ans = {}

        for _, v in foreign_key_map.items():
            if v["table_name"] not in ans:
                ans[v["table_name"]] = []
            ans[v["table_name"]].append(
                {k2: v2 for k2, v2 in v.items() if k2 != "table_name"}
            )
        return ans

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Gets all foreign keys for a table
        """
        schema = schema or self.default_schema_name

        # Use smart detection to decide between table-specific vs schema-wide query
        if self._should_use_table_specific_query(schema, **kw):
            return self._get_table_foreign_keys(connection, table_name, schema, **kw)

        # Use schema-wide cached query (optimal for full schema reflection)
        full_schema_name = self._get_full_schema_name(connection, schema, **kw)
        foreign_key_map = self._get_schema_foreign_keys(
            connection, self.denormalize_name(full_schema_name), **kw
        )
        return foreign_key_map.get(table_name, [])

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

    def _is_safe_for_desc_table(self, table_name):
        """
        Check if a table name is safe to use with DESC TABLE command.

        DESC TABLE is more restrictive than information_schema queries.
        Avoid optimization for table names with problematic characters.

        Args:
            table_name: The table name to check

        Returns:
            True if safe for DESC TABLE, False otherwise
        """
        # Avoid tables with characters that cause issues in DESC TABLE:
        # - Parentheses, brackets (require special escaping)
        # - Leading/trailing spaces
        # - Very complex quoted names
        problematic_chars = ["(", ")", "[", "]"]

        if any(char in str(table_name) for char in problematic_chars):
            return False

        # Avoid names that are just whitespace or have leading/trailing spaces
        if str(table_name) != str(table_name).strip():
            return False

        return True

    def _should_use_table_specific_query(self, schema, **kw):
        """
        Determine if we should use table-specific queries instead of schema-wide queries.

        Returns True when:
        1. cache_column_metadata=False (explicit user preference)
        2. Schema-wide columns are not yet cached AND table list is not cached
           (indicates probable single-table reflection)

        Returns False when:
        1. Schema-wide columns are already cached (use the cache!)
        2. Table names are already cached (indicates multi-table reflection in progress)
        3. We're doing a full schema reflection (metadata.reflect())
        """
        # Explicit opt-out via cache_column_metadata=False
        if not getattr(self, "_cache_column_metadata", True):
            return True

        # Check if we're in the middle of a multi-table reflection
        info_cache = kw.get("info_cache")
        if info_cache is None:
            return False  # No cache, use default schema-wide behavior

        # Check if schema-wide columns are already cached - if so, use them!
        # The cache key format used by @reflection.cache is (method, schema_name)
        schema_columns_key = (self._get_schema_columns.__name__, (schema,))
        if schema_columns_key in info_cache:
            return False  # Already have schema-wide data, use it

        # Check if table names have been fetched (indicates metadata.reflect() in progress)
        tables_info_key = (self._get_schema_tables_info.__name__, (schema,))
        if tables_info_key in info_cache:
            return False  # Multi-table reflection likely in progress

        # Don't use table-specific queries for full schema reflection
        # Check if we have _reflect_info which indicates metadata.reflect() is happening
        if "_reflect_info" in kw:
            return False  # Full schema reflection in progress

        # Cache is fresh and no table list yet - probably single-table reflection
        return True

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Gets all column info given the table info
        """
        schema = schema or self.default_schema_name
        if not schema:
            _, schema = self._current_database_schema(connection, **kw)

        # Optimization: For single-table reflection, use table-specific query
        # Skip optimization for edge cases (empty strings, special characters in DESC TABLE)
        # DESC TABLE is more restrictive than information_schema queries for certain characters
        if (
            self._should_use_table_specific_query(schema, **kw)
            and table_name
            and self._is_safe_for_desc_table(table_name)
        ):
            # Build fully-qualified table name with proper quoting
            full_table_name = self._denormalize_quote_join(schema, table_name)
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

        n2i = self.__class__._map_name_to_idx(cursor)
        try:
            ret = cursor.fetchone()
            if ret:
                return ret[n2i["text"]]
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
        n2i = self.__class__._map_name_to_idx(cursor)
        for row in cursor:
            if row[n2i["kind"]] == "TEMPORARY":
                ret.append(self.normalize_name(row[n2i["name"]]))

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

        n2i = self._map_name_to_idx(result)
        indexes = {}

        for row in result.cursor.fetchall():
            table_name = self.normalize_name(str(row[n2i["table"]]))
            if (
                row[n2i["name"]] == f'SYS_INDEX_{row[n2i["table"]]}_PRIMARY'
                or table_name not in filter_names
                or table_name not in hybrid_table_names
            ):
                continue
            index = {
                "name": row[n2i["name"]],
                "unique": row[n2i["is_unique"]] == "Y",
                "column_names": [
                    self.normalize_name(column)
                    for column in parse_index_columns(row[n2i["columns"]])
                ],
                "include_columns": [
                    self.normalize_name(column)
                    for column in parse_index_columns(row[n2i["included_columns"]])
                ],
                "dialect_options": {},
            }

            if (schema, table_name) in indexes:
                indexes[(schema, table_name)].append(index)
            else:
                indexes[(schema, table_name)] = [index]

        return list(indexes.items())

    def _value_or_default(self, data, table, schema):
        table = self.normalize_name(str(table))
        dic_data = dict(data)
        if (schema, table) in dic_data:
            return dic_data[(schema, table)]
        else:
            return []

    @reflection.cache
    def get_indexes(self, connection, tablename, schema, **kw):
        """
        Gets the indexes definition
        """
        table_name = self.normalize_name(str(tablename))
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
