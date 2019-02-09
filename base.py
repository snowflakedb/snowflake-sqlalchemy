#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2018 Snowflake Computing Inc. All right reserved.
#

import operator
import re
from collections import OrderedDict, Sequence
from functools import reduce

import sqlalchemy.types as sqltypes
from sqlalchemy import exc as sa_exc
from sqlalchemy import util as sa_util
from sqlalchemy import true, false
from sqlalchemy.engine import default, reflection
from sqlalchemy.schema import Table
from sqlalchemy.sql import (
    compiler, expression)
from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import quoted_name, ClauseElement
from sqlalchemy.types import (
    CHAR, DATE, DATETIME, INTEGER, SMALLINT, BIGINT, DECIMAL, TIME,
    TIMESTAMP, VARCHAR, BINARY, BOOLEAN, FLOAT, REAL)
from sqlalchemy.util.compat import string_types

from ..connector import errors as sf_errors
from ..connector.constants import UTF8

RESERVED_WORDS = frozenset([
    "ALL",  # ANSI Reserved words
    "ALTER",
    "AND",
    "ANY",
    "AS",
    "BETWEEN",
    "BY",
    "CHECK",
    "COLUMN",
    "CONNECT",
    "COPY",
    "CREATE",
    "CURRENT",
    "DELETE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "EXISTS",
    "FOR",
    "FROM",
    "GRANT",
    "GROUP",
    "HAVING",
    "IN",
    "INSERT",
    "INTERSECT",
    "INTO",
    "IS",
    "LIKE",
    "NOT",
    "NULL",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "REVOKE",
    "ROW",
    "ROWS",
    "SELECT",
    "SET",
    "START",
    "TABLE",
    "THEN",
    "TO",
    "TRIGGER",
    "UNION",
    "UNIQUE",
    "UPDATE",
    "VALUES",
    "WHENEVER",
    "WHERE",
    "WITH",
    "REGEXP", "RLIKE", "SOME",  # Snowflake Reserved words
    "MINUS", "INCREMENT",  # Oracle reserved words
])

colspecs = {
}


class VARIANT(sqltypes.TypeEngine):
    __visit_name__ = 'VARIANT'


class OBJECT(sqltypes.TypeEngine):
    __visit_name__ = 'OBJECT'


class ARRAY(sqltypes.TypeEngine):
    __visit_name__ = 'ARRAY'


class TIMESTAMP_TZ(sqltypes.TIMESTAMP):
    __visit_name__ = 'TIMESTAMP_TZ'


class TIMESTAMP_LTZ(sqltypes.TIMESTAMP):
    __visit_name__ = 'TIMESTAMP_LTZ'


class TIMESTAMP_NTZ(sqltypes.TIMESTAMP):
    __visit_name__ = 'TIMESTAMP_NTZ'


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

# Snowflake DML:
# - UPDATE
# - INSERT
# - DELETE
# - MERGE
AUTOCOMMIT_REGEXP = re.compile(
    r'\s*(?:UPDATE|INSERT|DELETE|MERGE|COPY)',
    re.I | re.UNICODE)


def translate_bool(bln):
    if bln:
        return true()
    return false()


NoneType = type(None)


class SnowflakeIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = set([x.lower() for x in RESERVED_WORDS])

    def __init__(self, dialect, **kw):
        quote = '"'

        super(SnowflakeIdentifierPreparer, self).__init__(
            dialect,
            initial_quote=quote,
            escape_quote=quote)

    def _quote_free_identifiers(self, *ids):
        """
        Unilaterally identifier-quote any number of strings.
        """
        return tuple([self.quote(i) for i in ids if i is not None])

    def quote_schema(self, schema, force=None):
        """
        Split schema by a dot and merge with required quotes
        """
        idents = self._split_schema_by_dot(schema)
        return '.'.join(self._quote_free_identifiers(*idents))

    def format_label(self, label, name=None):
        n = name or label.name
        s = n.replace(self.escape_quote, '')

        if not isinstance(n, quoted_name) or n.quote is None:
            return self.quote(s)

        return self.quote_identifier(s) if n.quote else s

    def _split_schema_by_dot(self, schema):
        ret = []
        idx = 0
        pre_idx = 0
        in_quote = False
        while idx < len(schema):
            if not in_quote:
                if schema[idx] == '.' and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    pre_idx = idx + 1
                elif schema[idx] == '"':
                    in_quote = True
                    pre_idx = idx + 1
            else:
                if schema[idx] == '"' and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    in_quote = False
                    pre_idx = idx + 1
            idx += 1
            if pre_idx < len(schema) and schema[pre_idx] == '.':
                pre_idx += 1
        if pre_idx < idx:
            ret.append(schema[pre_idx:idx])
        return ret


class SnowflakeCompiler(compiler.SQLCompiler):
    def visit_sequence(self, sequence, **kw):
        return (self.dialect.identifier_preparer.format_sequence(sequence) +
                ".nextval")

    def visit_merge_into(self, merge_into, **kw):
        clauses = " ".join(clause._compiler_dispatch(self, **kw) for clause in merge_into.clauses)
        return "MERGE INTO %s USING %s ON %s" % (merge_into.target, merge_into.source, merge_into.on) + (
            " " + clauses if clauses else ""
        )

    def visit_merge_into_clause(self, merge_into_clause, **kw):
        if merge_into_clause.command == 'INSERT':
            sets, sets_tos = zip(*merge_into_clause.set.items())
            return "WHEN NOT MATCHED%s THEN %s (%s) VALUES (%s)" % (
                " AND %s" % merge_into_clause.predicate._compiler_dispatch(
                    self, **kw) if merge_into_clause.predicate is not None else "",
                merge_into_clause.command,
                ", ".join(sets),
                ", ".join(map(lambda e: e._compiler_dispatch(self, **kw), sets_tos)))
        else:
            sets = ", ".join(
                ["%s = %s" % (set[0], set[1]._compiler_dispatch(self, **kw)) for set in
                 merge_into_clause.set.items()]) if merge_into_clause.set else ""
            return "WHEN MATCHED%s THEN %s%s" % (
                " AND %s" % merge_into_clause.predicate._compiler_dispatch(
                    self, **kw) if merge_into_clause.predicate is not None else "",
                merge_into_clause.command,
                " SET %s" % sets if merge_into_clause.set else "")

    def visit_copy_into(self, copy_into, **kw):
        formatter = copy_into.formatter._compiler_dispatch(self, **kw)
        into = copy_into.into._compiler_dispatch(self, **kw)
        from_ = copy_into.from_ if isinstance(copy_into.from_, Table) else '({})'.format(
            copy_into.from_._compiler_dispatch(self, **kw)
        )
        credentials, encryption = '', ''
        if isinstance(into, tuple):
            into, credentials, encryption = into
        elif isinstance(from_, tuple):
            from_, credentials, encryption = from_
        options = (' ' + ' '.join(["{} = {}".format(n, v._compiler_dispatch(self, **kw)) for n, v in
                                   copy_into.copy_options.items()])) if copy_into.copy_options else ''
        if credentials:
            options += " {}".format(credentials)
        if encryption:
            options += " {}".format(encryption)
        return "COPY INTO {} FROM {} {}{}".format(into, from_, formatter, options)

    def visit_copy_formatter(self, formatter, **kw):
        return 'FILE_FORMAT=(TYPE={}{})'.format(formatter.file_format,
                                                (' ' + ' '.join([("{}='{}'" if isinstance(value, str)
                                                                  else "{}={}").format(
                                                    name,
                                                    value._compiler_dispatch(self, **kw) if getattr(value,
                                                                                                    '_compiler_dispatch',
                                                                                                    False)
                                                    else str(value))
                                                    for name, value in
                                                    formatter.options.items()])) if formatter.options else "")

    def visit_aws_bucket(self, aws_bucket, **kw):
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in aws_bucket.credentials_used.items())
        )
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v) for n, v in
                     aws_bucket.encryption_used.items())
        )
        uri = "'s3://{}{}'".format(aws_bucket.bucket, '/' + aws_bucket.path if aws_bucket.path else "")
        return (uri,
                credentials if aws_bucket.credentials_used else '',
                encryption if aws_bucket.encryption_used else '')

    def visit_azure_container(self, azure_container, **kw):
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in azure_container.credentials_used.items())
        )
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v) for n, v in
                     azure_container.encryption_used.items())
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            azure_container.account,
            azure_container.container,
            '/' + azure_container.path if azure_container.path else ""
        )
        return (uri,
                credentials if azure_container.credentials_used else '',
                encryption if azure_container.encryption_used else '')

    def delete_extra_from_clause(self, delete_stmt, from_table,
                                 extra_froms, from_hints, **kw):
        return "USING " + ', '.join(
            t._compiler_dispatch(self, asfrom=True,
                                 fromhints=from_hints, **kw)
            for t in extra_froms)

    def update_from_clause(self, update_stmt, from_table,
                           extra_froms, from_hints, **kw):
        return "FROM " + ', '.join(
            t._compiler_dispatch(self, asfrom=True,
                                 fromhints=from_hints, **kw)
            for t in extra_froms)


class SnowflakeExecutionContext(default.DefaultExecutionContext):
    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "SELECT " +
            self.dialect.identifier_preparer.format_sequence(seq) +
            ".nextval", type_)

    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)

    @sa_util.memoized_property
    def should_autocommit(self):
        autocommit = self.execution_options.get(
            'autocommit',
            not self.compiled and self.statement
            and expression.PARSE_AUTOCOMMIT
            or False)

        if autocommit is expression.PARSE_AUTOCOMMIT:
            return self.should_autocommit_text(self.unicode_statement)
        else:
            return autocommit and not self.isddl


class SnowflakeDDLCompiler(compiler.DDLCompiler):
    def denormalize_column_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not \
                self.preparer._requires_quotes(name.lower()):
            # no quote as case insensitive
            return name
        return self.preparer.quote(name)

    def get_column_specification(self, column, **kwargs):
        """
        Gets Column specifications
        """
        colspec = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(
                column.type, type_expression=column)
        ]

        if not column.nullable:
            colspec.append('NOT NULL')

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append('DEFAULT ' + default)

        # TODO: This makes the first INTEGER column AUTOINCREMENT.
        # But the column is not really considered so unless
        # postfetch_lastrowid is enabled. But it is very unlikely to happen...
        if column.table is not None \
                and column is column.table._autoincrement_column and \
                column.server_default is None:
            colspec.append('AUTOINCREMENT')

        return ' '.join(colspec)

    def post_create_table(self, table):
        """
        Handles snowflake-specific ``CREATE TABLE ... CLUSTER BY`` syntax.

        Users can specify the `clusterby` property per table
        using the dialect specific syntax.
        For example, to specify a cluster by key you apply the following:

        >>> import sqlalchemy as sa
        >>> from sqlalchemy.schema import CreateTable
        >>> engine = sa.create_engine('snowflake://om1')
        >>> metadata = sa.MetaData()
        >>> user = sa.Table(
        ...     'user',
        ...     metadata,
        ...     sa.Column('id', sa.Integer, primary_key=True),
        ...     sa.Column('name', sa.String),
        ...     snowflake_clusterby=['id', 'name']
        ... )
        >>> print(CreateTable(user).compile(engine))
        <BLANKLINE>
        CREATE TABLE "user" (
            id INTEGER NOT NULL AUTOINCREMENT,
            name VARCHAR,
            PRIMARY KEY (id)
        ) CLUSTER BY (id, name)
        <BLANKLINE>
        <BLANKLINE>
        """
        text = ""
        info = table.dialect_options['snowflake']
        cluster = info.get('clusterby')
        if cluster:
            text += " CLUSTER BY ({0})".format(
                ", ".join(self.denormalize_column_name(key) for key in cluster))
        return text


class SnowflakeTypeCompiler(compiler.GenericTypeCompiler):
    def visit_VARIANT(self, type, **kw):
        return "VARIANT"

    def visit_ARRAY(self, type, **kw):
        return "ARRAY"

    def visit_OBJECT(self, type, **kw):
        return "OBJECT"

    def visit_BLOB(self, type, **kw):
        return "BINARY"

    def visit_datetime(self, type, **kw):
        return self.visit_TIMESTAMP(type, **kw)

    def visit_DATETIME(self, type, **kw):
        return self.visit_TIMESTAMP(type, **kw)

    def visit_TIMESTAMP_NTZ(self, type, **kw):
        kw['timezone'] = False
        return self.visit_TIMESTAMP(type, **kw)

    def visit_TIMESTAMP_TZ(self, type, **kw):
        kw['timezone'] = True
        return self.visit_TIMESTAMP(type, **kw)

    def visit_TIMESTAMP_LTZ(self, type, **kw):
        kw['timezone'] = True
        kw['is_local'] = True
        return self.visit_TIMESTAMP(type, **kw)

    def visit_TIMESTAMP(self, type, **kw):
        is_local = kw.get('is_local', False)
        timezone = kw.get('timezone', type.timezone)
        return "TIMESTAMP%s %s" % (
            "(%d)" % type.precision if getattr(type, 'precision',
                                               None) is not None else "",
            (timezone and "WITH" or "WITHOUT") + (
                    is_local and " LOCAL" or "") + " TIME ZONE"
        )


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
             is_identity) in result:
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
       ic.is_identity
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
       ic.is_identity
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


class MergeInto(UpdateBase):
    __visit_name__ = 'merge_into'
    _bind = None

    def __init__(self, target, source, on):
        self.target = target
        self.source = source
        self.on = on
        self.clauses = []

    class clause(ClauseElement):
        __visit_name__ = 'merge_into_clause'

        def __init__(self, command):
            self.set = None
            self.predicate = None
            self.command = command

        def __repr__(self):
            if self.command == 'INSERT':
                sets, sets_tos = zip(*self.set.items())
                return "WHEN NOT MATCHED%s THEN %s (%s) VALUES (%s)" % (
                    " AND %s" % self.predicate if self.predicate is not None else "",
                    self.command,
                    ", ".join(sets),
                    ", ".join(map(str, sets_tos)))
            else:
                # WHEN MATCHED clause
                sets = ", ".join(["%s = %s" % (set[0], set[1]) for set in self.set.items()]) if self.set else ""
                return "WHEN MATCHED%s THEN %s%s" % (" AND %s" % self.predicate if self.predicate is not None else "",
                                                     self.command,
                                                     " SET %s" % sets if self.set else "")

        def values(self, **kwargs):
            self.set = kwargs
            return self

        def where(self, expr):
            self.predicate = expr
            return self

    def __repr__(self):
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return "MERGE INTO %s USING %s ON %s" % (self.target, self.source, self.on) + (
            ' ' + clauses if clauses else ''
        )

    def when_matched_then_update(self):
        clause = self.clause('UPDATE')
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self):
        clause = self.clause('DELETE')
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self):
        clause = self.clause('INSERT')
        self.clauses.append(clause)
        return clause


class CopyInto(UpdateBase):
    """Copy Into Command base class, for documentation see:
    https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html"""

    __visit_name__ = 'copy_into'
    _bind = None

    def __init__(self, from_, into, formatter):
        self.from_ = from_
        self.into = into
        self.formatter = formatter
        self.copy_options = {}

    def __repr__(self):
        options = (' ' + ' '.join(["{} = {}".format(n, str(v)) for n, v in
                                   self.copy_options.items()])) if self.copy_options else ''
        return "COPY INTO {} FROM {} {}{}".format(self.into.__repr__(),
                                                  self.from_.__repr__(),
                                                  self.formatter.__repr__(),
                                                  options)

    def bind(self):
        return None

    def overwrite(self, overwrite):
        if not isinstance(overwrite, bool):
            raise TypeError("Parameter overwrite should  be a boolean value")
        self.copy_options.update({'OVERWRITE': translate_bool(overwrite)})

    def single(self, single_file):
        if not isinstance(single_file, bool):
            raise TypeError("Parameter single_file should  be a boolean value")
        self.copy_options.update({'SINGLE': translate_bool(single_file)})

    def maxfilesize(self, max_size):
        if not isinstance(max_size, bool):
            raise TypeError("Parameter max_size should  be a boolean value")
        self.copy_options.update({'MAX_FILE_SIZE': translate_bool(max_size)})


class CopyFormatter(ClauseElement):
    __visit_name__ = 'copy_formatter'

    def __init__(self):
        self.options = {}

    def __repr__(self):
        return 'FILE_FORMAT=(TYPE={}{})'.format(
            self.file_format,
            (' ' + ' '.join([("{} = '{}'" if isinstance(value, str) else "{} = {}").format(name, str(value))
                             for name, value in self.options.items()])) if self.options else ""
        )


class CSVFormatter(CopyFormatter):
    file_format = 'csv'

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = ['auto', 'gzip', 'bz2', 'brotli', 'zstd', 'deflate', 'raw_deflate', None]
        if comp_type not in _available_options:
            raise TypeError("Compression type should be one of : {}".format(_available_options))
        self.options['COMPRESSION'] = comp_type
        return self

    def record_delimiter(self, deli_type):
        """Character that separates records in an unloaded file."""
        if not isinstance(deli_type, (int, string_types)) \
                or (isinstance(deli_type, string_types) and len(deli_type) != 1):
            raise TypeError("Record delimeter should be a single character, that is either a string, or a number")
        if isinstance(deli_type, int):
            self.options['RECORD_DELIMITER'] = hex(deli_type)
        else:
            self.options['RECORD_DELIMITER'] = deli_type
        return self

    def field_delimeter(self, deli_type):
        """Character that separates fields in an unloaded file."""
        if not isinstance(deli_type, (int, NoneType, string_types)) \
                or (isinstance(deli_type, string_types) and len(deli_type) != 1):
            raise TypeError("Field delimeter should be a single character, that is either a string, or a number")
        if isinstance(deli_type, int):
            self.options['FIELD_DELIMETER'] = hex(deli_type)
        else:
            self.options['FIELD_DELIMETER'] = deli_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service. """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options['FILE_EXTENSION'] = ext
        return self

    def date_format(self, dt_frmt):
        """String that defines the format of date values in the unloaded data files."""
        if not isinstance(dt_frmt, string_types):
            raise TypeError("Date format should be a string")
        self.options['DATE_FORMAT'] = dt_frmt
        return self

    def time_format(self, tm_frmt):
        """String that defines the format of time values in the unloaded data files."""
        if not isinstance(tm_frmt, string_types):
            raise TypeError("Time format should be a string")
        self.options['TIME_FORMAT'] = tm_frmt
        return self

    def timestamp_format(self, tmstmp_frmt):
        """String that defines the format of timestamp values in the unloaded data files."""
        if not isinstance(tmstmp_frmt, string_types):
            raise TypeError("Timestamp format should be a string")
        self.options['TIMESTAMP_FORMAT'] = tmstmp_frmt
        return self

    def binary_format(self, bin_fmt):
        """Character used as the escape character for any field values. The option can be used when unloading data
        from binary columns in a table. """
        if isinstance(bin_fmt, string_types):
            bin_fmt = bin_fmt.lower()
        _available_options = ['hex', 'base64', 'utf8']
        if bin_fmt not in _available_options:
            raise TypeError("Binary format should be one of : {}".format(_available_options))
        self.options['BINARY_FORMAT'] = bin_fmt
        return self

    def escape(self, esc):
        """Character used as the escape character for any field values."""
        if not isinstance(esc, (int, NoneType, string_types)) \
                or (isinstance(esc, string_types) and len(esc) != 1):
            raise TypeError("Escape should be a single character, that is either a string, or a number")
        if isinstance(esc, int):
            self.options['ESCAPE'] = hex(esc)
        else:
            self.options['ESCAPE'] = esc
        return self

    def escape_unenclosed_field(self, esc):
        """Single character string used as the escape character for unenclosed field values only."""
        if not isinstance(esc, (int, NoneType, string_types)) \
                or (isinstance(esc, string_types) and len(esc) != 1):
            raise TypeError(
                "Escape unenclosed field should be a single character, that is either a string, or a number")
        if isinstance(esc, int):
            self.options['ESCAPE_UNENCLOSED_FIELD'] = hex(esc)
        else:
            self.options['ESCAPE_UNENCLOSED_FIELD'] = esc
        return self

    def field_optionally_enclosed_by(self, enc):
        """Character used to enclose strings. Either None, ', or \"."""
        _available_options = [None, '\'', '"']
        if enc not in _available_options:
            raise TypeError("Enclosing string should be one of : {}".format(_available_options))
        self.options['FIELD_OPTIONALLY_ENCLOSED_BY'] = enc
        return self

    def null_if(self, null):
        """Copying into a table these strings will be replaced by a NULL, while copying out of Snowflake will replace
        NULL values with the first string"""
        if not isinstance(null, Sequence):
            raise TypeError('Parameter null should be an iterable')
        self.options['NULL_IF'] = tuple(null)
        return self


class JSONFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = 'json'

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = ['auto', 'gzip', 'bz2', 'brotli', 'zstd', 'deflate', 'raw_deflate', None]
        if comp_type not in _available_options:
            raise TypeError("Compression type should be one of : {}".format(_available_options))
        self.options['COMPRESSION'] = comp_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service. """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options['FILE_EXTENSION'] = ext
        return self


class PARQUETFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = 'parquet'

    def snappy_compression(self, comp):
        """Enable, or disable snappy compression"""
        if not isinstance(comp, bool):
            raise TypeError("Comp should be a Boolean value")
        self.options['SNAPPY_COMPRESSION'] = translate_bool(comp)
        return self


class AWSBucket(ClauseElement):
    """AWS S3 bucket descriptor"""
    __visit_name__ = 'aws_bucket'

    def __init__(self, bucket, path=None):
        self.bucket = bucket
        self.path = path
        self.encryption_used = {}
        self.credentials_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:5] != 's3://':
            raise ValueError("Invalid AWS bucket URI: {}".format(uri))
        b = uri[5:].split('/', 1)
        if len(b) == 1:
            bucket, path = b[0], None
        else:
            bucket, path = b
        return cls(bucket, path)

    def __repr__(self):
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in self.credentials_used.items())
        )
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v)
                     for n, v in self.encryption_used.items())
        )
        uri = "'s3://{}{}'".format(self.bucket, '/' + self.path if self.path else "")
        return '{}{}{}'.format(uri,
                               ' ' + credentials if self.credentials_used else '',
                               ' ' + encryption if self.encryption_used else '')

    def credentials(self, aws_role=None, aws_key_id=None, aws_secret_key=None, aws_token=None):
        if aws_role is None and (aws_key_id is None and aws_secret_key is None):
            raise ValueError("Either 'aws_role', or aws_key_id and aws_secret_key has to be supplied")
        if aws_role:
            self.credentials_used = {'AWS_ROLE': aws_role}
        else:
            self.credentials_used = {'AWS_SECRET_KEY': aws_secret_key,
                                     'AWS_KEY_ID': aws_key_id}
            if aws_token:
                self.credentials_used['AWS_TOKEN'] = aws_token
        return self

    def encryption_aws_cse(self, master_key):
        self.encryption_used = {'TYPE': 'AWS_CSE',
                                'MASTER_KEY': master_key}
        return self

    def encryption_aws_sse_s3(self):
        self.encryption_used = {'TYPE': 'AWS_SSE_S3'}
        return self

    def encryption_aws_sse_kms(self, kms_key_id=None):
        self.encryption_used = {'TYPE': 'AWS_SSE_KMS'}
        if kms_key_id:
            self.encryption_used['KMS_KEY_ID'] = kms_key_id
        return self


class AzureContainer(ClauseElement):
    """Microsoft Azure Container descriptor"""
    __visit_name__ = 'azure_container'

    def __init__(self, account, container, path=None):
        self.account = account
        self.container = container
        self.path = path
        self.encryption_used = {}
        self.credential_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:8] != 'azure://':
            raise ValueError("Invalid Azure Container URI: {}".format(uri))
        account, uri = uri[8:].split('.', 1)
        if uri[0:22] != 'blob.core.windows.net/':
            raise ValueError("Invalid Azure Container URI: {}".format(uri))
        b = uri[22:].split('/', 1)
        if len(b) == 1:
            container, path = b[0], None
        else:
            container, path = b
        return cls(account, container, path)

    def __repr__(self):
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in self.credentials_used.items())
        )
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v) for n, v in
                     self.encryption_used.items())
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            self.account,
            self.container,
            '/' + self.path if self.path else ""
        )
        return uri + credentials if self.credentials_used else '' + encryption if self.encryption_used else ''

    def credentials(self, azure_sas_token):
        self.credentials_used = {'AZURE_SAS_TOKEN': azure_sas_token}
        return self

    def encryption_azure_cse(self, master_key):
        self.encryption_used = {'TYPE': 'AZURE_CSE', 'MASTER_KEY': master_key}
        return self


# NOTE: We only support CopyInto to external storage for now
CopyIntoStorage = CopyInto

dialect = SnowflakeDialect

construct_arguments = [
    (Table, {
        "clusterby": None
    })
]
