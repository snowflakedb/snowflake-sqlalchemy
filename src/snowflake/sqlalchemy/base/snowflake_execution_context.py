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

import re

from sqlalchemy import util as sa_util
from sqlalchemy.engine import default
from sqlalchemy.sql import expression

from ..util import _set_connection_interpolate_empty_sequences

# Snowflake DML:
# - UPDATE
# - INSERT
# - DELETE
# - MERGE
AUTOCOMMIT_REGEXP = re.compile(
    r"\s*(?:UPDATE|INSERT|DELETE|MERGE|COPY)", re.I | re.UNICODE
)


class SnowflakeExecutionContext(default.DefaultExecutionContext):
    INSERT_SQL_RE = re.compile(r"^insert\s+into", flags=re.IGNORECASE)

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            f"SELECT {self.identifier_preparer.format_sequence(seq)}.nextval",
            type_,
        )

    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)

    @sa_util.memoized_property
    def should_autocommit(self):
        autocommit = self.execution_options.get(
            "autocommit",
            not self.compiled
            and self.statement
            and expression.PARSE_AUTOCOMMIT
            or False,
        )

        if autocommit is expression.PARSE_AUTOCOMMIT:
            return self.should_autocommit_text(self.unicode_statement)
        else:
            return autocommit and not self.isddl

    def pre_exec(self):
        if self.compiled and self.identifier_preparer._double_percents:
            # for compiled statements, percent is doubled for escape, we turn on _interpolate_empty_sequences
            _set_connection_interpolate_empty_sequences(self._dbapi_connection, True)

            # if the statement is executemany insert, setting _interpolate_empty_sequences to True is not enough,
            # because executemany pre-processes the param binding and then pass None params to execute so
            # _interpolate_empty_sequences condition not getting met for the command.
            # Therefore, we manually revert the escape percent in the command here
            if self.executemany and self.INSERT_SQL_RE.match(self.statement):
                self.statement = self.statement.replace("%%", "%")
        else:
            # for other cases, do no interpolate empty sequences as "%" is not double escaped
            _set_connection_interpolate_empty_sequences(self._dbapi_connection, False)

    def post_exec(self):
        if self.compiled and self.identifier_preparer._double_percents:
            # for compiled statements, percent is doubled for escapeafter execution
            # we reset _interpolate_empty_sequences to false which is turned on in pre_exec
            _set_connection_interpolate_empty_sequences(self._dbapi_connection, False)

    @property
    def rowcount(self):
        return self.cursor.rowcount
