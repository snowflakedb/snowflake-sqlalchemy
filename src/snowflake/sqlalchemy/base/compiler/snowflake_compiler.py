# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import itertools
import operator

from sqlalchemy.schema import Table
from sqlalchemy.sql import compiler
from sqlalchemy.sql.selectable import Lateral

from snowflake.sqlalchemy.compat import string_types
from snowflake.sqlalchemy.custom_commands import (
    AWSBucket,
    AzureContainer,
    ExternalStage,
)


class SnowflakeCompiler(compiler.SQLCompiler):
    def visit_sequence(self, sequence, **kw):
        return self.dialect.identifier_preparer.format_sequence(sequence) + ".nextval"

    def visit_now_func(self, now, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_merge_into(self, merge_into, **kw):
        clauses = " ".join(
            clause._compiler_dispatch(self, **kw) for clause in merge_into.clauses
        )
        return (
            f"MERGE INTO {merge_into.target} USING {merge_into.source} ON {merge_into.on}"
            + (" " + clauses if clauses else "")
        )

    def visit_merge_into_clause(self, merge_into_clause, **kw):
        case_predicate = (
            f" AND {str(merge_into_clause.predicate._compiler_dispatch(self, **kw))}"
            if merge_into_clause.predicate is not None
            else ""
        )
        if merge_into_clause.command == "INSERT":
            sets, sets_tos = zip(*merge_into_clause.set.items())
            sets, sets_tos = list(sets), list(sets_tos)
            if kw.get("deterministic", False):
                sets, sets_tos = zip(
                    *sorted(merge_into_clause.set.items(), key=operator.itemgetter(0))
                )
            return "WHEN NOT MATCHED{} THEN {} ({}) VALUES ({})".format(
                case_predicate,
                merge_into_clause.command,
                ", ".join(sets),
                ", ".join(map(lambda e: e._compiler_dispatch(self, **kw), sets_tos)),
            )
        else:
            set_list = list(merge_into_clause.set.items())
            if kw.get("deterministic", False):
                set_list.sort(key=operator.itemgetter(0))
            sets = (
                ", ".join(
                    [
                        f"{set[0]} = {set[1]._compiler_dispatch(self, **kw)}"
                        for set in set_list
                    ]
                )
                if merge_into_clause.set
                else ""
            )
            return "WHEN MATCHED{} THEN {}{}".format(
                case_predicate,
                merge_into_clause.command,
                " SET %s" % sets if merge_into_clause.set else "",
            )

    def visit_copy_into(self, copy_into, **kw):
        if hasattr(copy_into, "formatter") and copy_into.formatter is not None:
            formatter = copy_into.formatter._compiler_dispatch(self, **kw)
        else:
            formatter = ""
        into = (
            copy_into.into
            if isinstance(copy_into.into, Table)
            else copy_into.into._compiler_dispatch(self, **kw)
        )
        from_ = None
        if isinstance(copy_into.from_, Table):
            from_ = copy_into.from_
        # this is intended to catch AWSBucket and AzureContainer
        elif (
            isinstance(copy_into.from_, AWSBucket)
            or isinstance(copy_into.from_, AzureContainer)
            or isinstance(copy_into.from_, ExternalStage)
        ):
            from_ = copy_into.from_._compiler_dispatch(self, **kw)
        # everything else (selects, etc.)
        else:
            from_ = f"({copy_into.from_._compiler_dispatch(self, **kw)})"
        credentials, encryption = "", ""
        if isinstance(into, tuple):
            into, credentials, encryption = into
        elif isinstance(from_, tuple):
            from_, credentials, encryption = from_
        options_list = list(copy_into.copy_options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        options = (
            (
                " "
                + " ".join(
                    [
                        "{} = {}".format(
                            n,
                            (
                                v._compiler_dispatch(self, **kw)
                                if getattr(v, "compiler_dispatch", False)
                                else str(v)
                            ),
                        )
                        for n, v in options_list
                    ]
                )
            )
            if copy_into.copy_options
            else ""
        )
        if credentials:
            options += f" {credentials}"
        if encryption:
            options += f" {encryption}"
        return f"COPY INTO {into} FROM {from_} {formatter}{options}"

    def visit_copy_formatter(self, formatter, **kw):
        options_list = list(formatter.options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        if "format_name" in formatter.options:
            return f"FILE_FORMAT=(format_name = {formatter.options['format_name']})"
        return "FILE_FORMAT=(TYPE={}{})".format(
            formatter.file_format,
            (
                " "
                + " ".join(
                    [
                        "{}={}".format(
                            name,
                            (
                                value._compiler_dispatch(self, **kw)
                                if hasattr(value, "_compiler_dispatch")
                                else formatter.value_repr(name, value)
                            ),
                        )
                        for name, value in options_list
                    ]
                )
                if formatter.options
                else ""
            ),
        )

    def visit_aws_bucket(self, aws_bucket, **kw):
        credentials_list = list(aws_bucket.credentials_used.items())
        if kw.get("deterministic", False):
            credentials_list.sort(key=operator.itemgetter(0))
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in credentials_list)
        )
        encryption_list = list(aws_bucket.encryption_used.items())
        if kw.get("deterministic", False):
            encryption_list.sort(key=operator.itemgetter(0))
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                ("{}='{}'" if isinstance(v, string_types) else "{}={}").format(n, v)
                for n, v in encryption_list
            )
        )
        uri = "'s3://{}{}'".format(
            aws_bucket.bucket, f"/{aws_bucket.path}" if aws_bucket.path else ""
        )
        return (
            uri,
            credentials if aws_bucket.credentials_used else "",
            encryption if aws_bucket.encryption_used else "",
        )

    def visit_azure_container(self, azure_container, **kw):
        credentials_list = list(azure_container.credentials_used.items())
        if kw.get("deterministic", False):
            credentials_list.sort(key=operator.itemgetter(0))
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in credentials_list)
        )
        encryption_list = list(azure_container.encryption_used.items())
        if kw.get("deterministic", False):
            encryption_list.sort(key=operator.itemgetter(0))
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                f"{n}='{v}'" if isinstance(v, string_types) else f"{n}={v}"
                for n, v in encryption_list
            )
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            azure_container.account,
            azure_container.container,
            f"/{azure_container.path}" if azure_container.path else "",
        )
        return (
            uri,
            credentials if azure_container.credentials_used else "",
            encryption if azure_container.encryption_used else "",
        )

    def visit_external_stage(self, external_stage, **kw):
        if external_stage.file_format is None:
            return (
                f"@{external_stage.namespace}{external_stage.name}{external_stage.path}"
            )
        return f"@{external_stage.namespace}{external_stage.name}{external_stage.path} (file_format => {external_stage.file_format})"

    def delete_extra_from_clause(
        self, delete_stmt, from_table, extra_froms, from_hints, **kw
    ):
        return "USING " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    def update_from_clause(
        self, update_stmt, from_table, extra_froms, from_hints, **kw
    ):
        return "FROM " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    def _get_regexp_args(self, binary, kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is not None:
            flags = self.process(flags, **kw)
        return string, pattern, flags

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        if flags is None:
            return f"REGEXP_LIKE({string}, {pattern})"
        else:
            return f"REGEXP_LIKE({string}, {pattern}, {flags})"

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        try:
            replacement = self.process(binary.modifiers["replacement"], **kw)
        except KeyError:
            # in sqlalchemy 1.4.49, the internal structure of the expression is changed
            # that binary.modifiers doesn't have "replacement":
            # https://docs.sqlalchemy.org/en/20/changelog/changelog_14.html#change-1.4.49
            return f"REGEXP_REPLACE({string}, {pattern}{'' if flags is None else f', {flags}'})"

        if flags is None:
            return f"REGEXP_REPLACE({string}, {pattern}, {replacement})"
        else:
            return f"REGEXP_REPLACE({string}, {pattern}, {replacement}, {flags})"

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return f"NOT {self.visit_regexp_match_op_binary(binary, operator, **kw)}"

    def visit_join(self, join, asfrom=False, from_linter=None, **kwargs):
        if from_linter:
            from_linter.edges.update(
                itertools.product(join.left._from_objects, join.right._from_objects)
            )

        if join.full:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        else:
            join_type = " JOIN "

        join_statement = (
            join.left._compiler_dispatch(
                self, asfrom=True, from_linter=from_linter, **kwargs
            )
            + join_type
            + join.right._compiler_dispatch(
                self, asfrom=True, from_linter=from_linter, **kwargs
            )
        )

        if join.onclause is None and isinstance(join.right, Lateral):
            # in snowflake, onclause is not accepted for lateral due to BCR change:
            # https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
            # sqlalchemy only allows join with on condition.
            # to adapt to snowflake syntax change,
            # we make the change such that when oncaluse is None and the right part is
            # Lateral, we do not append the on condition
            return join_statement

        return (
            join_statement
            + " ON "
            # TODO: likely need asfrom=True here?
            + join.onclause._compiler_dispatch(self, from_linter=from_linter, **kwargs)
        )

    def render_literal_value(self, value, type_):
        # escape backslash
        return super().render_literal_value(value, type_).replace("\\", "\\\\")
