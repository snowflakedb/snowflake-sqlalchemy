#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from typing import Any

from sqlalchemy import exc
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.schema import SchemaItem, Table

from snowflake.sqlalchemy.constants import DIALECT_NAME

from .table_option_base import TableOptionBase


class TableOption(TableOptionBase, SchemaItem):
    def _set_parent(self, parent: SchemaEventTarget, **kw: Any) -> None:
        if self.__option_name__ == "default":
            raise exc.SQLAlchemyError(f"{self.__class__.__name__} does not has a name")
        if not isinstance(parent, Table):
            raise exc.SQLAlchemyError(
                f"{self.__class__.__name__} option can only be applied to Table"
            )
        parent.dialect_options[DIALECT_NAME][self.__option_name__] = self

    def _set_table_option_parent(self, parent: SchemaEventTarget, **kw: Any) -> None:
        pass
