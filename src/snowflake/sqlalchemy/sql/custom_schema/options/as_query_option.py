#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional, Union

from sqlalchemy.sql import Selectable

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey


class AsQueryOption(TableOption):
    """Class to represent an AS clause in tables.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas

    Example:
        as_query=AsQueryOption('select name, address from existing_table where name = "test"')

        is equivalent to:

        as select name, address from existing_table where name = "test"
    """

    def __init__(self, query: Union[str, Selectable]) -> None:
        super().__init__()
        self._name: TableOptionKey = TableOptionKey.AS_QUERY
        self.query = query

    @staticmethod
    def create(
        value: Optional[Union["AsQueryOption", str, Selectable]]
    ) -> "TableOption":
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

    def __get_expression(self):
        if isinstance(self.query, Selectable):
            return self.query.compile(compile_kwargs={"literal_binds": True})
        return self.query

    def _render(self, compiler) -> str:
        return self.template() % (self.__get_expression())

    def __repr__(self) -> str:
        return "AsQueryOption(%s)" % self.__get_expression()


AsQueryOptionType = Union[AsQueryOption, str, Selectable]
