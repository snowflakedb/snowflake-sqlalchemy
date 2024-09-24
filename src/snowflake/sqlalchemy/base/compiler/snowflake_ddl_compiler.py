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

from sqlalchemy import Sequence
from sqlalchemy.sql import compiler


class SnowflakeDDLCompiler(compiler.DDLCompiler):
    def denormalize_column_name(self, name):
        if name is None:
            return None
        elif name.lower() == name and not self.preparer._requires_quotes(name.lower()):
            # no quote as case insensitive
            return name
        return self.preparer.quote(name)

    def get_column_specification(self, column, **kwargs):
        """
        Gets Column specifications
        """
        colspec = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(column.type, type_expression=column),
        ]

        has_identity = (
            column.identity is not None and self.dialect.supports_identity_columns
        )

        if not column.nullable:
            colspec.append("NOT NULL")

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append("DEFAULT " + default)

        # TODO: This makes the first INTEGER column AUTOINCREMENT.
        # But the column is not really considered so unless
        # postfetch_lastrowid is enabled. But it is very unlikely to happen...
        if (
            column.table is not None
            and column is column.table._autoincrement_column
            and column.server_default is None
        ):
            if isinstance(column.default, Sequence):
                colspec.append(
                    f"DEFAULT {self.dialect.identifier_preparer.format_sequence(column.default)}.nextval"
                )
            else:
                colspec.append("AUTOINCREMENT")

        if has_identity:
            colspec.append(self.process(column.identity))

        return " ".join(colspec)

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
        info = table.dialect_options["snowflake"]
        cluster = info.get("clusterby")
        if cluster:
            text += " CLUSTER BY ({})".format(
                ", ".join(self.denormalize_column_name(key) for key in cluster)
            )
        return text

    def visit_create_stage(self, create_stage, **kw):
        """
        This visitor will create the SQL representation for a CREATE STAGE command.
        """
        return "CREATE {or_replace}{temporary}STAGE {}{} URL={}".format(
            create_stage.stage.namespace,
            create_stage.stage.name,
            repr(create_stage.container),
            or_replace="OR REPLACE " if create_stage.replace_if_exists else "",
            temporary="TEMPORARY " if create_stage.temporary else "",
        )

    def visit_create_file_format(self, file_format, **kw):
        """
        This visitor will create the SQL representation for a CREATE FILE FORMAT
        command.
        """
        return "CREATE {}FILE FORMAT {} TYPE='{}' {}".format(
            "OR REPLACE " if file_format.replace_if_exists else "",
            file_format.format_name,
            file_format.formatter.file_format,
            " ".join(
                [
                    f"{name} = {file_format.formatter.value_repr(name, value)}"
                    for name, value in file_format.formatter.options.items()
                ]
            ),
        )

    def visit_drop_table_comment(self, drop, **kw):
        """Snowflake does not support setting table comments as NULL.

        Reflection has to account for this and convert any empty comments to NULL.
        """
        table_name = self.preparer.format_table(drop.element)
        return f"COMMENT ON TABLE {table_name} IS ''"

    def visit_drop_column_comment(self, drop, **kw):
        """Snowflake does not support directly setting column comments as NULL.

        Instead we are forced to use the ALTER COLUMN ... UNSET COMMENT instead.
        """
        return "ALTER TABLE {} ALTER COLUMN {} UNSET COMMENT".format(
            self.preparer.format_table(drop.element.table),
            self.preparer.format_column(drop.element),
        )

    def visit_identity_column(self, identity, **kw):
        text = "IDENTITY"
        if identity.start is not None or identity.increment is not None:
            start = 1 if identity.start is None else identity.start
            increment = 1 if identity.increment is None else identity.increment
            text += f"({start},{increment})"
        if identity.order is not None:
            order = "ORDER" if identity.order else "NOORDER"
            text += f" {order}"
        return text

    def get_identity_options(self, identity_options):
        text = []
        if identity_options.increment is not None:
            text.append("INCREMENT BY %d" % identity_options.increment)
        if identity_options.start is not None:
            text.append("START WITH %d" % identity_options.start)
        if identity_options.minvalue is not None:
            text.append("MINVALUE %d" % identity_options.minvalue)
        if identity_options.maxvalue is not None:
            text.append("MAXVALUE %d" % identity_options.maxvalue)
        if identity_options.nominvalue is not None:
            text.append("NO MINVALUE")
        if identity_options.nomaxvalue is not None:
            text.append("NO MAXVALUE")
        if identity_options.cache is not None:
            text.append("CACHE %d" % identity_options.cache)
        if identity_options.cycle is not None:
            text.append("CYCLE" if identity_options.cycle else "NO CYCLE")
        if identity_options.order is not None:
            text.append("ORDER" if identity_options.order else "NOORDER")
        return " ".join(text)
