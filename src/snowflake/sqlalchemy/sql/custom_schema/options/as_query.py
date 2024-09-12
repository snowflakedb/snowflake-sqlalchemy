#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Union

from sqlalchemy import Selectable

from .table_option import TableOption
from .table_option_base import Priority


class AsQuery(TableOption):
    """Class to represent an AS clause in tables.
    This configuration option is used to specify the query from which the table is created.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-table#create-table-as-select-also-referred-to-as-ctas


    AsQuery example usage using an input string:
    DynamicTable(
        "sometable", metadata,
        Column("name", String(50)),
        Column("address", String(100)),
        AsQuery('select name, address from existing_table where name = "test"')
        )

    AsQuery example usage using a selectable statement:
    DynamicTable(
        "sometable",
        Base.metadata,
        TargetLag(10, TimeUnit.SECONDS),
        Warehouse("warehouse"),
        AsQuery(select(test_table_1).where(test_table_1.c.id == 23))
    )

    """

    __option_name__ = "as_query"
    __priority__ = Priority.LOWEST

    def __init__(self, query: Union[str, Selectable]) -> None:
        r"""Construct an as_query object.

        :param \*expressions:
           AS <query>

        """
        self.query = query

    @staticmethod
    def template() -> str:
        return "AS %s"

    def get_expression(self):
        if isinstance(self.query, Selectable):
            return self.query.compile(compile_kwargs={"literal_binds": True})
        return self.query

    def render_option(self, compiler) -> str:
        return AsQuery.template() % (self.get_expression())

    def __repr__(self) -> str:
        return "Query(%s)" % self.get_expression()
