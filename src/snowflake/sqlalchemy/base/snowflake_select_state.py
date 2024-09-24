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

import itertools

from sqlalchemy import exc as sa_exc
from sqlalchemy import util as sa_util
from sqlalchemy.sql.base import CompileState
from sqlalchemy.sql.selectable import SelectState

from ..util import _find_left_clause_to_join_from, _Snowflake_Selectable_Join


# handle Snowflake BCR bcr-1057
@CompileState.plugin_for("default", "select")
class SnowflakeSelectState(SelectState):
    def _setup_joins(self, args, raw_columns):
        for right, onclause, left, flags in args:
            isouter = flags["isouter"]
            full = flags["full"]

            if left is None:
                (
                    left,
                    replace_from_obj_index,
                ) = self._join_determine_implicit_left_side(
                    raw_columns, left, right, onclause
                )
            else:
                (replace_from_obj_index) = self._join_place_explicit_left_side(left)

            if replace_from_obj_index is not None:
                # splice into an existing element in the
                # self._from_obj list
                left_clause = self.from_clauses[replace_from_obj_index]

                self.from_clauses = (
                    self.from_clauses[:replace_from_obj_index]
                    + (
                        _Snowflake_Selectable_Join(  # handle Snowflake BCR bcr-1057
                            left_clause,
                            right,
                            onclause,
                            isouter=isouter,
                            full=full,
                        ),
                    )
                    + self.from_clauses[replace_from_obj_index + 1 :]
                )
            else:
                self.from_clauses = self.from_clauses + (
                    # handle Snowflake BCR bcr-1057
                    _Snowflake_Selectable_Join(
                        left, right, onclause, isouter=isouter, full=full
                    ),
                )

    @sa_util.preload_module("sqlalchemy.custom_commands.util")
    def _join_determine_implicit_left_side(self, raw_columns, left, right, onclause):
        """When join conditions don't express the left side explicitly,
        determine if an existing FROM or entity in this query
        can serve as the left hand side.

        """

        replace_from_obj_index = None

        from_clauses = self.from_clauses

        if from_clauses:
            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(from_clauses, right, onclause)

            if len(indexes) == 1:
                replace_from_obj_index = indexes[0]
                left = from_clauses[replace_from_obj_index]
        else:
            potential = {}
            statement = self.statement

            for from_clause in itertools.chain(
                itertools.chain.from_iterable(
                    [element._from_objects for element in raw_columns]
                ),
                itertools.chain.from_iterable(
                    [element._from_objects for element in statement._where_criteria]
                ),
            ):

                potential[from_clause] = ()

            all_clauses = list(potential.keys())
            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(all_clauses, right, onclause)

            if len(indexes) == 1:
                left = all_clauses[indexes[0]]

        if len(indexes) > 1:
            raise sa_exc.InvalidRequestError(
                "Can't determine which FROM clause to join "
                "from, there are multiple FROMS which can "
                "join to this entity. Please use the .select_from() "
                "method to establish an explicit left side, as well as "
                "providing an explicit ON clause if not present already to "
                "help resolve the ambiguity."
            )
        elif not indexes:
            raise sa_exc.InvalidRequestError(
                "Don't know how to join to %r. "
                "Please use the .select_from() "
                "method to establish an explicit left side, as well as "
                "providing an explicit ON clause if not present already to "
                "help resolve the ambiguity." % (right,)
            )
        return left, replace_from_obj_index
