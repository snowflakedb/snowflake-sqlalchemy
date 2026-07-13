#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Union

from snowflake.sqlalchemy.custom_commands import NoneType
from sqlalchemy.sql import Selectable

from .table_option import Priority, TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class AsQueryOption(TableOption):
    """Class to represent an AS clause in tables.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas

    Example:
        as_query=AsQueryOption('select name, address from existing_table where name = "test"')

        is equivalent to:

        as select name, address from existing_table where name = "test"
    """

    def __init__(self, query: str | Selectable) -> None:
        super().__init__()
        self._name: TableOptionKey = TableOptionKey.AS_QUERY
        self.query = query

    @staticmethod
    def create(  # type: ignore[override]
        value: AsQueryOption | str | Selectable | None,
    ) -> TableOption | None:
        if isinstance(value, (NoneType, AsQueryOption)):
            return value
        if isinstance(value, (str, Selectable)):
            return AsQueryOption(value)
        return TableOption._get_invalid_table_option(
            TableOptionKey.AS_QUERY,
            str(type(value).__name__),
            [AsQueryOption.__name__, str.__name__, Selectable.__name__],
        )

    def template(self) -> str:
        return "AS %s"

    @property
    def priority(self) -> Priority:
        return Priority.LOWEST

    def __get_expression(self, compiler=None):
        if isinstance(self.query, Selectable):
            dialect = compiler.dialect if compiler is not None else None
            return self.query.compile(
                dialect=dialect,
                compile_kwargs={"literal_binds": True},
            )
        return self.query

    def _render(self, compiler: SnowflakeDDLCompiler) -> str:
        return self.template() % (self.__get_expression(compiler))

    def __repr__(self) -> str:
        return "AsQueryOption(%s)" % self.__get_expression()


AsQueryOptionType = Union[AsQueryOption, str, Selectable]
