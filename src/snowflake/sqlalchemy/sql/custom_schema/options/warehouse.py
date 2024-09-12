#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Optional

from .table_option import TableOption
from .table_option_base import Priority


class Warehouse(TableOption):
    """Class to represent the warehouse clause.
    This configuration option is used to specify the warehouse for the dynamic table.
    For further information on this clause, please refer to: https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table


    Warehouse example usage:
    DynamicTable("sometable", metadata,
                    Column("name", String(50)),
                    Column("address", String(100)),
                    Warehouse('my_warehouse_name')
                    )
    """

    __option_name__ = "warehouse"
    __priority__ = Priority.HIGH

    def __init__(
        self,
        name: Optional[str],
    ) -> None:
        r"""Construct a Warehouse object.

        :param \*expressions:
          Dynamic table warehouse option.
           WAREHOUSE = <warehouse_name>

        """
        self.name = name

    @staticmethod
    def template() -> str:
        return "WAREHOUSE = %s"

    def get_expression(self):
        return self.name

    def render_option(self, compiler) -> str:
        return Warehouse.template() % (self.get_expression())

    def __repr__(self) -> str:
        return "Warehouse(%s)" % self.get_expression()
