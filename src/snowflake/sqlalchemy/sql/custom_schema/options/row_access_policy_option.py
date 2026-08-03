#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
from __future__ import annotations

from typing import TYPE_CHECKING, Union

from snowflake.sqlalchemy.custom_commands import NoneType

from .table_option import Priority, TableOption, TableOptionKey

if TYPE_CHECKING:
    from snowflake.sqlalchemy.base import SnowflakeDDLCompiler


class RowAccessPolicyOption(TableOption):
    """Class to represent a row access policy option in Snowflake Tables.

    Takes a tuple of (policy_name, list_of_columns) where:
    - policy_name: str - name of the row access policy
    - list_of_columns: list[str] - list of columns the policy applies to

    Example:
        row_access_policy = RowAccessPolicyOption('my_policy', ['col1', 'col2'])

        is equivalent to:

        WITH ROW ACCESS POLICY my_policy ON (col1, col2)
    """

    def __init__(self, policy_name: str, columns: list[str]) -> None:
        super().__init__()
        self.policy_name: str = policy_name
        self.columns: list[str] = columns

    @property
    def priority(self) -> Priority:
        # Render after CLUSTER BY: Snowflake's CREATE TABLE grammar places the
        # row access policy clause after CLUSTER BY (options are emitted highest
        # priority first), so this must be lower than ClusterByOption (HIGH).
        return Priority.LOW

    @staticmethod
    def create(  # type: ignore[override]
        name: TableOptionKey,
        value: tuple[str, list[str]] | RowAccessPolicyOption | None,
    ) -> TableOption | None:
        if isinstance(value, NoneType):
            return None

        if isinstance(value, tuple) and len(value) == 2:
            policy_name, columns = value
            if isinstance(policy_name, str) and isinstance(columns, list):
                if all(isinstance(col, str) for col in columns):
                    value = RowAccessPolicyOption(policy_name, columns)
                else:
                    return TableOption._get_invalid_table_option(
                        name,
                        str(type(value).__name__),
                        [RowAccessPolicyOption.__name__, "tuple[str, list[str]]"],
                    )
            else:
                return TableOption._get_invalid_table_option(
                    name,
                    str(type(value).__name__),
                    [RowAccessPolicyOption.__name__, "tuple[str, list[str]]"],
                )

        if isinstance(value, RowAccessPolicyOption):
            value._set_option_name(name)
            return value

        return TableOption._get_invalid_table_option(
            name,
            str(type(value).__name__),
            [RowAccessPolicyOption.__name__, "tuple[str, list[str]]"],
        )

    def template(self) -> str:
        name = self.option_name
        assert name is not None, f"option_name not set on {self.__class__.__name__}"
        return f"WITH {name.upper()} %s ON (%s)"

    def _render(self, compiler: SnowflakeDDLCompiler) -> str:
        policy_name = self._quote_identifier_value(self.policy_name, compiler)
        columns_str = ", ".join(
            self._quote_identifier_value(col, compiler) for col in self.columns
        )
        return self.template() % (policy_name, columns_str)

    def __repr__(self) -> str:
        option_name = (
            f", table_option_key={self.option_name}"
            if not isinstance(self.option_name, NoneType)
            else ""
        )
        return f"RowAccessPolicyOption(policy_name='{self.policy_name}', columns={self.columns}{option_name})"


RowAccessPolicyOptionType = Union[RowAccessPolicyOption, tuple[str, list[str]]]
