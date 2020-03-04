#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import operator
import re

from sqlalchemy import util as sa_util
from sqlalchemy.engine import default
from sqlalchemy.schema import Table
from sqlalchemy.sql import compiler, expression
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.util.compat import string_types

from .custom_commands import AWSBucket, AzureContainer

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
    "SAMPLE",
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

# Snowflake DML:
# - UPDATE
# - INSERT
# - DELETE
# - MERGE
AUTOCOMMIT_REGEXP = re.compile(
    r'\s*(?:UPDATE|INSERT|DELETE|MERGE|COPY)',
    re.I | re.UNICODE)


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
            sets, sets_tos = list(sets), list(sets_tos)
            if kw.get('deterministic', False):
                sets, sets_tos = zip(*sorted(merge_into_clause.set.items(), key=operator.itemgetter(0)))
            return "WHEN NOT MATCHED%s THEN %s (%s) VALUES (%s)" % (
                " AND %s" % merge_into_clause.predicate._compiler_dispatch(
                    self, **kw) if merge_into_clause.predicate is not None else "",
                merge_into_clause.command,
                ", ".join(sets),
                ", ".join(map(lambda e: e._compiler_dispatch(self, **kw), sets_tos)))
        else:
            set_list = list(merge_into_clause.set.items())
            if kw.get('deterministic', False):
                set_list.sort(key=operator.itemgetter(0))
            sets = ", ".join(
                ["%s = %s" % (set[0], set[1]._compiler_dispatch(self, **kw)) for set in
                 set_list]) if merge_into_clause.set else ""
            return "WHEN MATCHED%s THEN %s%s" % (
                " AND %s" % merge_into_clause.predicate._compiler_dispatch(
                    self, **kw) if merge_into_clause.predicate is not None else "",
                merge_into_clause.command,
                " SET %s" % sets if merge_into_clause.set else "")

    def visit_copy_into(self, copy_into, **kw):
        formatter = copy_into.formatter._compiler_dispatch(self, **kw)
        into = (copy_into.into if isinstance(copy_into.into, Table)
                else copy_into.into._compiler_dispatch(self, **kw))
        from_ = None
        if isinstance(copy_into.from_, Table):
            from_ = copy_into.from_
        # this is intended to catch AWSBucket and AzureContainer
        elif isinstance(copy_into.from_, AWSBucket) or isinstance(copy_into.from_, AzureContainer):
            from_ = copy_into.from_._compiler_dispatch(self, **kw)
        # everything else (selects, etc.)
        else:
            from_ = '({})'.format(copy_into.from_._compiler_dispatch(self,
                                                                     **kw))
        credentials, encryption = '', ''
        if isinstance(into, tuple):
            into, credentials, encryption = into
        elif isinstance(from_, tuple):
            from_, credentials, encryption = from_
        options_list = list(copy_into.copy_options.items())
        if kw.get('deterministic', False):
            options_list.sort(key=operator.itemgetter(0))
        options = (' ' + ' '.join(["{} = {}".format(
            n, v._compiler_dispatch(self, **kw) if getattr(v, 'compiler_dispatch', False) else str(v)
        ) for n, v in options_list])) if copy_into.copy_options else ''
        if credentials:
            options += " {}".format(credentials)
        if encryption:
            options += " {}".format(encryption)
        return "COPY INTO {} FROM {} {}{}".format(into, from_, formatter, options)

    def visit_copy_formatter(self, formatter, **kw):
        options_list = list(formatter.options.items())
        if kw.get('deterministic', False):
            options_list.sort(key=operator.itemgetter(0))
        return 'FILE_FORMAT=(TYPE={}{})'.format(formatter.file_format,
                                                (' ' + ' '.join([("{}='{}'" if isinstance(value, str)
                                                                  else "{}={}").format(
                                                    name,
                                                    value._compiler_dispatch(self, **kw) if getattr(value,
                                                                                                    '_compiler_dispatch',
                                                                                                    False) else str(
                                                        value))
                                                    for name, value in options_list])) if formatter.options else "")

    def visit_external_stage(self, stage, **kw):
        return "@{}{}{}".format(stage.namespace, stage.name, stage.path)

    def visit_aws_bucket(self, aws_bucket, **kw):
        credentials_list = list(aws_bucket.credentials_used.items())
        if kw.get('deterministic', False):
            credentials_list.sort(key=operator.itemgetter(0))
        credentials = 'CREDENTIALS=({})'.format(
            ' '.join("{}='{}'".format(n, v) for n, v in credentials_list)
        )
        encryption_list = list(aws_bucket.encryption_used.items())
        if kw.get('deterministic', False):
            encryption_list.sort(key=operator.itemgetter(0))
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v) for n, v in encryption_list)
        )
        uri = "'s3://{}{}'".format(aws_bucket.bucket, '/' + aws_bucket.path if aws_bucket.path else "")
        return (uri,
                credentials if aws_bucket.credentials_used else '',
                encryption if aws_bucket.encryption_used else '')

    def visit_azure_container(self, azure_container, **kw):
        credentials_list = list(azure_container.credentials_used.items())
        if kw.get('deterministic', False):
            credentials_list.sort(key=operator.itemgetter(0))
        credentials = 'CREDENTIALS=({})'.format(' '.join("{}='{}'".format(n, v) for n, v in credentials_list))
        encryption_list = list(azure_container.encryption_used.items())
        if kw.get('deterministic', False):
            encryption_list.sort(key=operator.itemgetter(0))
        encryption = 'ENCRYPTION=({})'.format(
            ' '.join(("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v) for n, v in
                     encryption_list)
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
            not self.compiled and self.statement and expression.PARSE_AUTOCOMMIT or False)

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
    def visit_BYTEINT(self, type_, **kw):
        return "BYTEINT"

    def visit_CHARACTER(self, type_, **kw):
        return "CHARACTER"

    def visit_DEC(self, type_, **kw):
        return "DEC"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_FIXED(self, type_, **kw):
        return "FIXED"

    def visit_INT(self, type_, **kw):
        return "INT"

    def visit_NUMBER(self, type_, **kw):
        return "NUMBER"

    def visit_STRING(self, type_, **kw):
        return "STRING"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_VARIANT(self, type_, **kw):
        return "VARIANT"

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY"

    def visit_OBJECT(self, type_, **kw):
        return "OBJECT"

    def visit_BLOB(self, type_, **kw):
        return "BINARY"

    def visit_datetime(self, type_, **kw):
        return "datetime"

    def visit_DATETIME(self, type_, **kw):
        return "DATETIME"

    def visit_TIMESTAMP_NTZ(self, type_, **kw):
        return "TIMESTAMP_NTZ"

    def visit_TIMESTAMP_TZ(self, type_, **kw):
        return "TIMESTAMP_TZ"

    def visit_TIMESTAMP_LTZ(self, type_, **kw):
        return "TIMESTAMP_LTZ"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"


construct_arguments = [
    (Table, {
        "clusterby": None
    })
]
