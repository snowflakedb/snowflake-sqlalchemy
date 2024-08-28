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

from sqlalchemy.sql.dml import UpdateBase
from sqlalchemy.sql.elements import ClauseElement


class MergeInto(UpdateBase):
    __visit_name__ = "merge_into"
    _bind = None

    def __init__(self, target, source, on):
        self.target = target
        self.source = source
        self.on = on
        self.clauses = []

    class clause(ClauseElement):
        __visit_name__ = "merge_into_clause"

        def __init__(self, command):
            self.set = {}
            self.predicate = None
            self.command = command

        def __repr__(self):
            case_predicate = (
                f" AND {str(self.predicate)}" if self.predicate is not None else ""
            )
            if self.command == "INSERT":
                sets, sets_tos = zip(*self.set.items())
                return "WHEN NOT MATCHED{} THEN {} ({}) VALUES ({})".format(
                    case_predicate,
                    self.command,
                    ", ".join(sets),
                    ", ".join(map(str, sets_tos)),
                )
            else:
                # WHEN MATCHED clause
                sets = (
                    ", ".join([f"{set[0]} = {set[1]}" for set in self.set.items()])
                    if self.set
                    else ""
                )
                return "WHEN MATCHED{} THEN {}{}".format(
                    case_predicate,
                    self.command,
                    f" SET {str(sets)}" if self.set else "",
                )

        def values(self, **kwargs):
            self.set = kwargs
            return self

        def where(self, expr):
            self.predicate = expr
            return self

    def __repr__(self):
        clauses = " ".join([repr(clause) for clause in self.clauses])
        return f"MERGE INTO {self.target} USING {self.source} ON {self.on}" + (
            f" {clauses}" if clauses else ""
        )

    def when_matched_then_update(self):
        clause = self.clause("UPDATE")
        self.clauses.append(clause)
        return clause

    def when_matched_then_delete(self):
        clause = self.clause("DELETE")
        self.clauses.append(clause)
        return clause

    def when_not_matched_then_insert(self):
        clause = self.clause("INSERT")
        self.clauses.append(clause)
        return clause
