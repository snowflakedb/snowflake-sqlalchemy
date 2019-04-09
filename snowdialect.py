#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import operator
from collections import OrderedDict
import sqlalchemy.types as sqltypes
from sqlalchemy import util as sa_util
from sqlalchemy import event as sa_vnt
from functools import reduce
from sqlalchemy import exc as sa_exc
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.schema import Table
from sqlalchemy.engine import default, reflection
from sqlalchemy.types import (
    CHAR, DATE, DATETIME, INTEGER, SMALLINT, BIGINT, DECIMAL, TIME, TIMESTAMP, VARCHAR, BINARY, BOOLEAN, FLOAT, REAL
)

from .base import (
    SnowflakeDDLCompiler, SnowflakeCompiler, SnowflakeExecutionContext, SnowflakeIdentifierPreparer,
    SnowflakeTypeCompiler
)
from snowflake.connector.constants import UTF8
from .custom_types import (
    TIMESTAMP_LTZ, TIMESTAMP_TZ, TIMESTAMP_NTZ, VARIANT, OBJECT, ARRAY
)
from snowflake.connector import errors as sf_errors

colspecs = {}

ischema_names = {
    'BIGINT': BIGINT,
    'BINARY': BINARY,
    # 'BIT': BIT,
    'BOOLEAN': BOOLEAN,
    'CHAR': CHAR,
    'CHARACTER': CHAR,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'DEC': DECIMAL,
    'DECIMAL': DECIMAL,
    'DOUBLE': FLOAT,
    'FIXED': DECIMAL,
    'FLOAT': FLOAT,
    'INT': INTEGER,
    'INTEGER': INTEGER,
    'NUMBER': DECIMAL,
    # 'OBJECT': ?
    'REAL': REAL,
    'BYTEINT': SMALLINT,
    'SMALLINT': SMALLINT,
    'STRING': VARCHAR,
    'TEXT': VARCHAR,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'TIMESTAMP_TZ': TIMESTAMP_TZ,
    'TIMESTAMP_LTZ': TIMESTAMP_LTZ,
    'TIMESTAMP_NTZ': TIMESTAMP_NTZ,
    'TINYINT': SMALLINT,
    'VARBINARY': BINARY,
    'VARCHAR': VARCHAR,
    'VARIANT': VARIANT,
    'OBJECT': OBJECT,
    'ARRAY': ARRAY,
}


class SnowflakeDialect(default.DefaultDialect):
    name = 'snowflake'
    max_identifier_length = 65535

    encoding = UTF8
    default_paramstyle = 'pyformat'
    colspecs = colspecs
    ischema_names = ischema_names

    # all str types must be converted in Unicode
    convert_unicode = True

    # Indicate whether the DB-API can receive SQL statements as Python
    #  unicode strings
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
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

    preparer = SnowflakeIdentifierPreparer
    ddl_compiler = SnowflakeDDLCompiler
    type_compiler = SnowflakeTypeCompiler
    statement_compiler = SnowflakeCompiler
    execution_ctx_cls = SnowflakeExecutionContext

    # indicates symbol names are UPPERCASEd if they are case insensitive
    # within the database. If this is True, the methods normalize_name()
    # and denormalize_name() must be provided.
    requires_name_normalize = True

    @classmethod
    def dbapi(cls):
        from snowflake import connector
        return connector

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'database' in opts:
            name_spaces = opts['database'].split('/')
            if len(name_spaces) == 1:
                pass
            elif len(name_spaces) == 2:
                opts['database'] = name_spaces[0]
                opts['schema'] = name_spaces[1]
            else:
                raise sa_exc.ArgumentError(
                    "Invalid name space is specified: {0}".format(
                        opts['database']
                    ))
        if '.snowflakecomputing.com' not in opts['host'] and not opts.get(
                'port'):
            opts['account'] = opts['host']
            if u'.' in opts['account']:
                # remove region subdomain
                opts['account'] = opts['account'][0:opts['account'].find(u'.')]
            opts['host'] = opts['host'] + '.snowflakecomputing.com'
            opts['port'] = '443'
        opts['autocommit'] = False  # autocommit is disabled by default
        opts.update(url.query)
        self._cache_column_metadata = opts.get('cache_column_metadata',
                                               "false").lower() == 'true'
        return ([], opts)

    def has_table(self, connection, table_name, schema=None):
        """
        Checks if the table exists
        """
        return self._has_object(connection, 'TABLE', table_name, schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        """
        Checks if the sequence exists
        """
        return self._has_object(connection, 'SEQUENCE', sequence_name, schema)

    def _has_object(self, connection, object_type, object_name, schema=None):

        full_name = self._denormalize_quote_join(schema, object_name)
        try:
            results = connection.execute(
                "DESC {0} /* sqlalchemy:_has_object */ {1}".format(
                    object_type, full_name))
            row = results.fetchone()
            have = row is not None
            return have
        except sa_exc.DBAPIError as e:
            if e.orig.__class__ == sf_errors.ProgrammingError:
                return False
            raise

    def normalize_name(self, name):
        if name is None:
            return None
        if name.upper() == name and not \
                self.identifier_preparer._requires_quotes(name.lower()):
            return name.lower()
        elif name.lower() == name:
            return quoted_name(name, quote=True)
        else:
            return name

    def denormalize_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not \
                self.identifier_preparer._requires_quotes(name.lower()):
            name = name.upper()
        return name

    def _denormalize_quote_join(self, *idents):
        ip = self.identifier_preparer
        split_idents = reduce(
            operator.add,
            [ip._split_schema_by_dot(ids) for ids in idents if ids is not None])
        return '.'.join(
            ip._quote_free_identifiers(*split_idents))

    def _current_database_schema(self, connection):
        con = connection.connect().connection
        return (
            self.normalize_name(con.database),
            self.normalize_name(con.schema))

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
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Gets all indexes
        """
        # no index is supported by Snowflake
        return []

    @reflection.cache
    def _describe_table(self, connection, full_table_name, **_):
        result = connection.execute(
            "DESCRIBE TABLE /* sqlalchemy:get_primary_keys */ {0}".format(
                self.denormalize_name(full_table_name)))
        n2i = self.__class__._map_name_to_idx(result)
        return result.fetchall(), n2i

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):

        schema = schema or self.default_schema_name
        if not schema:
            _, schema = self._current_database_schema(connection)

        full_table_name = self._denormalize_quote_join(schema, table_name)

        result, n2i = self._describe_table(connection, full_table_name, **kw)

        primary_key_info = {
            'constrained_columns': [],
            'name': None  # optional
        }
        for row in result:
            column_name = row[n2i['name']]
            is_primary_key = row[n2i['primary key']] == 'Y'
            if is_primary_key:
                primary_key_info['constrained_columns'].append(
                    self.normalize_name(column_name))

        return primary_key_info

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Gets all foreign keys
        """
        schema = schema or self.default_schema_name
        current_database, current_schema = self._current_database_schema(
            connection)
        full_schema_name = self._denormalize_quote_join(
            current_database, schema if schema else current_schema)

        result = connection.execute(
            "SHOW /* sqlalchemy:get_foreign_keys */ IMPORTED KEYS "
            "IN SCHEMA {0}".format(self.denormalize_name(full_schema_name))
        )
        n2i = self.__class__._map_name_to_idx(result)

        foreign_key_map = {}
        for row in result:
            name = self.normalize_name(row[n2i['fk_name']])
            constrained_table = self.normalize_name(row[n2i['fk_table_name']])
            if constrained_table == table_name:
                constrained_column = self.normalize_name(
                    row[n2i['fk_column_name']])
                referred_schema = self.normalize_name(
                    row[n2i['pk_schema_name']])
                referred_table = self.normalize_name(row[n2i['pk_table_name']])
                referred_column = self.normalize_name(
                    row[n2i['pk_column_name']])

                if not name in foreign_key_map:
                    foreign_key_map[name] = {
                        'constrained_columns': [constrained_column],
                        'referred_schema': referred_schema,
                        'referred_table': referred_table,
                        'referred_columns': [referred_column],
                    }
                else:
                    foreign_key_map[name]['constrained_columns'].append(
                        constrained_column)
                    foreign_key_map[name]['referred_columns'].append(
                        referred_column)
        ret = []
        for name in foreign_key_map:
            foreign_key = {
                'name': name,
            }
            foreign_key.update(foreign_key_map[name])
            ret.append(foreign_key)
        return ret

    def _get_columns_for_table_query(
            self, connection, query, table_schema=None, table_name=None):
        params = {
            'table_schema': self.denormalize_name(table_schema),
            'table_name': self.denormalize_name(table_name),
        }
        result = connection.execute(query, params)
        for (table_name,
             colname,
             coltype,
             character_maximum_length,
             numeric_precision,
             numeric_scale,
             is_nullable,
             column_default,
             is_identity,
             comment) in result:
            table_name = self.normalize_name(table_name)
            colname = self.normalize_name(colname)
            if colname.startswith('sys_clustering_column'):
                # ignoring clustering column
                continue
            col_type = self.ischema_names.get(coltype, None)
            col_type_kw = {}
            if col_type is None:
                sa_util.warn(
                    "Did not recognize type '{}' of column '{}'".format(
                        coltype, colname))
                col_type = sqltypes.NULLTYPE
            else:
                if issubclass(col_type, FLOAT):
                    col_type_kw['precision'] = numeric_precision
                    col_type_kw['decimal_return_scale'] = numeric_scale
                elif issubclass(col_type, sqltypes.Numeric):
                    col_type_kw['precision'] = numeric_precision
                    col_type_kw['scale'] = numeric_scale
                elif issubclass(col_type,
                                (sqltypes.String, sqltypes.BINARY)):
                    col_type_kw['length'] = character_maximum_length

            type_instance = col_type(**col_type_kw)

            yield (table_name, colname, {
                'name': colname,
                'type': type_instance,
                'nullable': is_nullable == 'YES',
                'default': column_default,
                'autoincrement': is_identity == 'YES',
                'comment': comment,
            })

    @reflection.cache
    def _get_columns_for_tables(self, connection, schema, **kw):
        info_cache = kw.get('info_cache', None)
        if info_cache is None:
            return

        query = """
SELECT /* sqlalchemy:_get_columns_for_tables */
       ic.table_name,
       ic.column_name,
       ic.data_type,
       ic.character_maximum_length,
       ic.numeric_precision,
       ic.numeric_scale,
       ic.is_nullable,
       ic.column_default,
       ic.is_identity,
       ic.comment
  FROM information_schema.columns ic
 WHERE ic.table_schema=%(table_schema)s
"""
        for table_name, colname, obj in self._get_columns_for_table_query(
                connection, query, table_schema=schema, table_name=None):
            sfkey = ('snowflake_cache', schema, table_name)
            if info_cache.get(sfkey) is None:
                info_cache[sfkey] = {}

            info_cache[sfkey][colname] = obj

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Gets all column info given the table info
        """
        schema = schema or self.default_schema_name
        if not schema:
            _, schema = self._current_database_schema(connection)

        full_table_name = self._denormalize_quote_join(schema, table_name)

        result, n2i = self._describe_table(connection, full_table_name, **kw)

        column_map = OrderedDict()
        for row in result:
            # NOTE: ideally information_schema include primary key info as well
            column_name = self.normalize_name(row[n2i['name']])
            is_primary_key = row[n2i['primary key']] == 'Y'
            column_map[column_name] = is_primary_key

        columns = []

        info_cache = kw.get('info_cache', None)
        sfkey = ('snowflake_cache', schema, table_name)
        if info_cache is None or info_cache.get(sfkey) is None:
            # no column metadata in cache
            query = """
SELECT /* sqlalchemy:get_columns */
       ic.table_name,
       ic.column_name,
       ic.data_type,
       ic.character_maximum_length,
       ic.numeric_precision,
       ic.numeric_scale,
       ic.is_nullable,
       ic.column_default,
       ic.is_identity,
       ic.comment
  FROM information_schema.columns ic
 WHERE ic.table_schema=%(table_schema)s
   AND ic.table_name=%(table_name)s
 ORDER BY ic.ordinal_position
"""
            # no column metadata cache is found
            for _, colname, obj in self._get_columns_for_table_query(
                    connection, query, table_schema=schema,
                    table_name=table_name):
                obj['primary_key'] = column_map[colname]
                columns.append(obj)
        else:
            # found column metadata in cache produced by get_table_names
            cache = info_cache[sfkey]
            for colname, is_primary_key in column_map.items():
                column_metadata = cache.get(colname)
                # combine with output from DESCRIBE TABLE
                column_metadata['primary_key'] = is_primary_key
                columns.append(column_metadata)
            del info_cache[sfkey]  # no longer need it

        return columns

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Gets all table names.
        """
        schema = schema or self.default_schema_name
        current_schema = schema
        if schema:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_table_names */ TABLES IN {0}".format(
                    self._denormalize_quote_join(schema)))
        else:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_table_names */ TABLES")
            _, current_schema = self._current_database_schema(connection)

        ret = [self.normalize_name(row[1]) for row in cursor]

        # special flag to cache all column metadata for all tables in a schema.
        if hasattr(self, "_cache_column_metadata") and \
                self._cache_column_metadata:
            self._get_columns_for_tables(connection, current_schema, **kw)
        return ret

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        Gets all view names
        """
        schema = schema or self.default_schema_name
        if schema:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_view_names */ VIEWS IN {0}".format(
                    self._denormalize_quote_join((schema))))
        else:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_view_names */ VIEWS")

        return [self.normalize_name(row[1]) for row in cursor]

    @reflection.cache
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """
        Gets the view definition
        """
        schema = schema or self.default_schema_name
        if schema:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_view_definition */ VIEWS "
                "LIKE '{0}' IN {1}".format(
                    self._denormalize_quote_join(view_name),
                    self._denormalize_quote_join(schema)))
        else:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_view_definition */ VIEWS "
                "LIKE '{0}'".format(
                    self._denormalize_quote_join(view_name)))

        n2i = self.__class__._map_name_to_idx(cursor)
        try:
            ret = cursor.fetchone()
            if ret:
                return ret[n2i['text']]
        except:
            pass
        return None

    def get_temp_table_names(self, connection, schema=None, **kw):
        schema = schema or self.default_schema_name
        if schema:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_temp_table_names */ TABLES "
                "IN {0}".format(
                    self._denormalize_quote_join(schema)))
        else:
            cursor = connection.execute(
                "SHOW /* sqlalchemy:get_temp_table_names */ TABLES")

        ret = []
        n2i = self.__class__._map_name_to_idx(cursor)
        for row in cursor:
            if row[n2i['kind']] == 'TEMPORARY':
                ret.append(self.normalize_name(row[n2i['name']]))

        return ret

    def get_schema_names(self, connection, **kw):
        """
        Gets all schema names.
        """
        cursor = connection.execute(
            "SHOW /* sqlalchemy:get_schema_names */ SCHEMAS")

        return [self.normalize_name(row[1]) for row in cursor]


@sa_vnt.listens_for(Table, 'before_create')
def check_table(table, connection, _ddl_runner, **kw):
    if isinstance(_ddl_runner.dialect, SnowflakeDialect) and table.indexes:
        raise NotImplementedError("Snowflake does not support indexes")

dialect = SnowflakeDialect
