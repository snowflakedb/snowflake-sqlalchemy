#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Union

from snowflake.sqlalchemy.custom_commands import NoneType
from sqlalchemy.sql.expression import TextClause

from .table_option import Priority, TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class ClusterByOption(TableOption):
    """Class to represent the cluster by clause in tables.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/user-guide/tables-clustering-keys
    Example:
        cluster_by=ClusterByOption('name', text('id > 0'))

        is equivalent to:

        cluster by (name, id > 0)
    """

    def __init__(self, *expressions: str | TextClause) -> None:
        super().__init__()
        self._name: TableOptionKey = TableOptionKey.CLUSTER_BY
        self.expressions = expressions

    @staticmethod
    def create(  # type: ignore[override]
        value: ClusterByOptionType | None,
    ) -> TableOption | None:
        if isinstance(value, (NoneType, ClusterByOption)):
            return value
        if isinstance(value, list):
            return ClusterByOption(*value)
        return TableOption._get_invalid_table_option(
            TableOptionKey.CLUSTER_BY,
            str(type(value).__name__),
            [ClusterByOption.__name__, list.__name__],
        )

    def template(self) -> str:
        name = self.option_name
        assert name is not None, f"option_name not set on {self.__class__.__name__}"
        return f"{name.upper()} (%s)"

    @property
    def priority(self) -> Priority:
        return Priority.HIGH

    def __get_expression(self) -> str:
        return ", ".join([str(expression) for expression in self.expressions])

    def _render(self, compiler: SnowflakeDDLCompiler) -> str:
        return self.template() % (self.__get_expression())

    def __repr__(self) -> str:
        return "ClusterByOption(%s)" % self.__get_expression()


ClusterByOptionType = Union[ClusterByOption, list[Union[str, TextClause]]]
