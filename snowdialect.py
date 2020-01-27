#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import operator
from six.moves.urllib_parse import unquote_plus

import sqlalchemy.types as sqltypes
from six import iteritems
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
from snowflake.connector import errors as sf_errors, ProgrammingError

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
    driver = 'snowflake'
    max_identifier_length = 65535
    cte_follows_insert = True

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

    @classmethod
    def dbapi(cls):
        from snowflake import connector
        return connector

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        if 'database' in opts:
            name_spaces = [unquote_plus(e) for e in opts['database'].split('/')]
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
                # remove external ID
                opts['account'] = opts['account'].split('-')[0]
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

    @reflection.cache
    def _current_database_schema(self, connection, **kw):
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
    def _get_schema_primary_keys(self, connection, schema, **kw):
        result = connection.execute(
            "SHOW /* sqlalchemy:_get_schema_primary_keys */PRIMARY KEYS IN SCHEMA {0}".format(schema)
        )
        ans = {}
        for row in result:
            table_name = self.normalize_name(row['table_name'])
            if table_name not in ans:
                ans[table_name] = {'constrained_columns': [], 'name': None}
            ans[table_name]['constrained_columns'].append(self.normalize_name(row['column_name']))
        return ans

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        schema = schema or self.default_schema_name
        current_database, current_schema = self._current_database_schema(connection, **kw)
        full_schema_name = self._denormalize_quote_join(
            current_database, schema if schema else current_schema)
        return self._get_schema_primary_keys(
            connection,
            self.denormalize_name(full_schema_name),
            **kw
        ).get(table_name, {'constrained_columns': [],
                           'name': None})

    @reflection.cache
    def _get_schema_foreign_keys(self, connection, schema, **kw):
        result = connection.execute(
            "SHOW /* sqlalchemy:_get_schema_foreign_keys */ IMPORTED KEYS IN SCHEMA {0}".format(schema)
        )
        foreign_key_map = {}
        for row in result:
            name = self.normalize_name(row['fk_name'])
            if name not in foreign_key_map:
                foreign_key_map[name] = {
                    'constrained_columns': [self.normalize_name(row['fk_column_name'])],
                    'referred_schema': self.normalize_name(row['pk_schema_name']),
                    'referred_table': self.normalize_name(row['pk_table_name']),
                    'referred_columns': [self.normalize_name(row['pk_column_name'])],
                    'name': name,
                    'table_name': self.normalize_name(row['fk_table_name'])
                }
            else:
                foreign_key_map[name]['constrained_columns'].append(self.normalize_name(row['fk_column_name']))
                foreign_key_map[name]['referred_columns'].append(self.normalize_name(row['pk_column_name']))

        ans = {}
        for k, v in iteritems(foreign_key_map):
            if v['table_name'] not in ans:
                ans[v['table_name']] = []
            ans[v['table_name']].append({k2: v2 for k2, v2 in iteritems(v) if k2 != 'table_name'})
        return ans

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Gets all foreign keys for a table
        """
        schema = schema or self.default_schema_name
        current_database, current_schema = self._current_database_schema(connection, **kw)
        full_schema_name = self._denormalize_quote_join(
            current_database, schema if schema else current_schema)

        foreign_key_map = self._get_schema_foreign_keys(connection, self.denormalize_name(full_schema_name), **kw)
        return foreign_key_map.get(table_name, [])

    @reflection.cache
    def _get_schema_columns(self, connection, schema, **kw):
        """Get all columns in the schema, if we hit 'Information schema query returned too much data' problem return
        None, as it is cacheable and is an unexpected return type for this function"""
        ans = {}
        current_database, _ = self._current_database_schema(connection, **kw)
        full_schema_name = self._denormalize_quote_join(current_database, schema)
        schema_primary_keys = self._get_schema_primary_keys(connection, full_schema_name, **kw)
        try:
            result = connection.execute("""
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
                   ic.comment
              FROM information_schema.columns ic
             WHERE ic.table_schema=%(table_schema)s
             ORDER BY ic.ordinal_position""", {"table_schema": self.denormalize_name(schema)})
        except ProgrammingError as pe:
            if pe.errno == 90030:
                # This means that there are too many tables in the schema, we need to go more granular
                return None  # None triggers _get_table_columns while staying cacheable
            raise
        for (table_name,
             column_name,
             coltype,
             character_maximum_length,
             numeric_precision,
             numeric_scale,
             is_nullable,
             column_default,
             is_identity,
             comment) in result:
            table_name = self.normalize_name(table_name)
            column_name = self.normalize_name(column_name)
            if table_name not in ans:
                ans[table_name] = list()
            if column_name.startswith('sys_clustering_column'):
                continue  # ignoring clustering column
            col_type = self.ischema_names.get(coltype, None)
            col_type_kw = {}
            if col_type is None:
                sa_util.warn(
                    "Did not recognize type '{}' of column '{}'".format(
                        coltype, column_name))
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

            current_table_pks = schema_primary_keys.get(table_name)

            ans[table_name].append({
                'name': column_name,
                'type': type_instance,
                'nullable': is_nullable == 'YES',
                'default': column_default,
                'autoincrement': is_identity == 'YES',
                'comment': comment,
                'primary_key': (column_name in schema_primary_keys[table_name]['constrained_columns']) if current_table_pks else False,
            })
        return ans

    @reflection.cache
    def _get_table_columns(self, connection, table_name, schema=None, **kw):
        """Get all columns in a table in a schema"""
        ans = []
        current_database, _ = self._current_database_schema(connection, **kw)
        full_schema_name = self._denormalize_quote_join(current_database, schema)
        schema_primary_keys = self._get_schema_primary_keys(connection, full_schema_name, **kw)
        result = connection.execute("""
        SELECT /* sqlalchemy:get_table_columns */
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
         ORDER BY ic.ordinal_position""", {"table_schema": self.denormalize_name(schema),
                                           "table_name": self.denormalize_name(table_name)})
        for (table_name,
             column_name,
             coltype,
             character_maximum_length,
             numeric_precision,
             numeric_scale,
             is_nullable,
             column_default,
             is_identity,
             comment) in result:
            table_name = self.normalize_name(table_name)
            column_name = self.normalize_name(column_name)
            if column_name.startswith('sys_clustering_column'):
                continue  # ignoring clustering column
            col_type = self.ischema_names.get(coltype, None)
            col_type_kw = {}
            if col_type is None:
                sa_util.warn(
                    "Did not recognize type '{}' of column '{}'".format(
                        coltype, column_name))
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

            current_table_pks = schema_primary_keys.get(table_name)

            ans.append({
                'name': column_name,
                'type': type_instance,
                'nullable': is_nullable == 'YES',
                'default': column_default,
                'autoincrement': is_identity == 'YES',
                'comment': comment,
                'primary_key': (column_name in schema_primary_keys[table_name][
                    'constrained_columns']) if current_table_pks else False,
            })
        return ans

    def get_columns(self, connection, table_name, schema=None, **kw):
        """
        Gets all column info given the table info
        """
        schema = schema or self.default_schema_name
        if not schema:
            _, schema = self._current_database_schema(connection, **kw)

        schema_columns = self._get_schema_columns(connection, schema, **kw)
        if schema_columns is None:
            # Too many results, fall back to only query about single table
            return self._get_table_columns(connection, table_name, schema, **kw)
        return schema_columns[self.normalize_name(table_name)]

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

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        """
        Returns comment of table in a dictionary as described by SQLAlchemy spec
        """
        sql_command = "SHOW /* sqlalchemy:get_table_comment */ " \
                      "TABLES LIKE '{0}'{1}".format(
                                        table_name,
                                        (' IN SCHEMA {0}'.format(self.normalize_name(schema))) if schema else ''
                                    )
        cursor = connection.execute(sql_command)
        ans = cursor.fetchone()
        return {'text': ans['comment']}


@sa_vnt.listens_for(Table, 'before_create')
def check_table(table, connection, _ddl_runner, **kw):
    if isinstance(_ddl_runner.dialect, SnowflakeDialect) and table.indexes:
        raise NotImplementedError("Snowflake does not support indexes")


dialect = SnowflakeDialect
