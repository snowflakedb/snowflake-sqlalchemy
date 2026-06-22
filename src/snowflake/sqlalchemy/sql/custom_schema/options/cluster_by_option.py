#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import List, Union

from sqlalchemy.sql.expression import TextClause

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey


class ClusterByOption(TableOption):
    """Class to represent the cluster by clause in tables.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/user-guide/tables-clustering-keys
    Example:
        cluster_by=ClusterByOption('name', text('id > 0'))

        is equivalent to:

        cluster by (name, id > 0)
    """

    def __init__(self, *expressions: Union[str, TextClause]) -> None:
        super().__init__()
        self._name: TableOptionKey = TableOptionKey.CLUSTER_BY
        self.expressions = expressions

    @staticmethod
    def create(value: "ClusterByOptionType") -> "TableOption":
        if isinstance(value, (NoneType, ClusterByOption)):
            return value
        if isinstance(value, List):
            return ClusterByOption(*value)
        return TableOption._get_invalid_table_option(
            TableOptionKey.CLUSTER_BY,
            str(type(value).__name__),
            [ClusterByOption.__name__, list.__name__],
        )

    def template(self) -> str:
        return f"{self.option_name.upper()} (%s)"

    @property
    def priority(self) -> Priority:
        return Priority.HIGH

    def __get_expression(self, compiler=None):
        parts = []
        for expr in self.expressions:
            if isinstance(expr, TextClause):
                parts.append(str(expr))  # TextClause is trusted literal SQL
            elif isinstance(expr, str):
                parts.append(self._quote_identifier_value(expr, compiler))
            else:
                raise TypeError(
                    "ClusterByOption expressions must be str or TextClause, "
                    f"got {type(expr).__name__}"
                )
        return ", ".join(parts)

    def _render(self, compiler) -> str:
        return self.template() % (self.__get_expression(compiler))

    def __repr__(self) -> str:
        return "ClusterByOption(%s)" % self.__get_expression()


ClusterByOptionType = Union[ClusterByOption, List[Union[str, TextClause]]]
