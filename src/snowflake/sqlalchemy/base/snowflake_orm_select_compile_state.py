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

"""
Overwrite methods to handle Snowflake BCR change:
https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
- _join_determine_implicit_left_side
- _join_left_to_right
"""

from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect, sql
from sqlalchemy.orm import context
from sqlalchemy.orm.context import _MapperEntity

from ..compat import IS_VERSION_20, args_reducer
from ..util import _find_left_clause_to_join_from, _Snowflake_ORMJoin


# handle Snowflake BCR bcr-1057
@sql.base.CompileState.plugin_for("orm", "select")
class SnowflakeORMSelectCompileState(context.ORMSelectCompileState):
    def _join_determine_implicit_left_side(
        self, entities_collection, left, right, onclause
    ):
        """When join conditions don't express the left side explicitly,
        determine if an existing FROM or entity in this query
        can serve as the left hand side.

        """

        # when we are here, it means join() was called without an ORM-
        # specific way of telling us what the "left" side is, e.g.:
        #
        # join(RightEntity)
        #
        # or
        #
        # join(RightEntity, RightEntity.foo == LeftEntity.bar)
        #

        r_info = inspect(right)

        replace_from_obj_index = use_entity_index = None

        if self.from_clauses:
            # we have a list of FROMs already.  So by definition this
            # join has to connect to one of those FROMs.

            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(
                self.from_clauses, r_info.selectable, onclause
            )

            if len(indexes) == 1:
                replace_from_obj_index = indexes[0]
                left = self.from_clauses[replace_from_obj_index]
            elif len(indexes) > 1:
                raise sa_exc.InvalidRequestError(
                    "Can't determine which FROM clause to join "
                    "from, there are multiple FROMS which can "
                    "join to this entity. Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity."
                )
            else:
                raise sa_exc.InvalidRequestError(
                    "Don't know how to join to %r. "
                    "Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity." % (right,)
                )

        elif entities_collection:
            # we have no explicit FROMs, so the implicit left has to
            # come from our list of entities.

            potential = {}
            for entity_index, ent in enumerate(entities_collection):
                entity = ent.entity_zero_or_selectable
                if entity is None:
                    continue
                ent_info = inspect(entity)
                if ent_info is r_info:  # left and right are the same, skip
                    continue

                # by using a dictionary with the selectables as keys this
                # de-duplicates those selectables as occurs when the query is
                # against a series of columns from the same selectable
                if isinstance(ent, context._MapperEntity):
                    potential[ent.selectable] = (entity_index, entity)
                else:
                    potential[ent_info.selectable] = (None, entity)

            all_clauses = list(potential.keys())
            # handle Snowflake BCR bcr-1057
            indexes = _find_left_clause_to_join_from(
                all_clauses, r_info.selectable, onclause
            )

            if len(indexes) == 1:
                use_entity_index, left = potential[all_clauses[indexes[0]]]
            elif len(indexes) > 1:
                raise sa_exc.InvalidRequestError(
                    "Can't determine which FROM clause to join "
                    "from, there are multiple FROMS which can "
                    "join to this entity. Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity."
                )
            else:
                raise sa_exc.InvalidRequestError(
                    "Don't know how to join to %r. "
                    "Please use the .select_from() "
                    "method to establish an explicit left side, as well as "
                    "providing an explicit ON clause if not present already "
                    "to help resolve the ambiguity." % (right,)
                )
        else:
            raise sa_exc.InvalidRequestError(
                "No entities to join from; please use "
                "select_from() to establish the left "
                "entity/selectable of this join"
            )

        return left, replace_from_obj_index, use_entity_index

    @args_reducer(positions_to_drop=(6, 7))
    def _join_left_to_right(
        self, entities_collection, left, right, onclause, prop, outerjoin, full
    ):
        """given raw "left", "right", "onclause" parameters consumed from
        a particular key within _join(), add a real ORMJoin object to
        our _from_obj list (or augment an existing one)

        """

        if left is None:
            # left not given (e.g. no relationship object/name specified)
            # figure out the best "left" side based on our existing froms /
            # entities
            assert prop is None
            (
                left,
                replace_from_obj_index,
                use_entity_index,
            ) = self._join_determine_implicit_left_side(
                entities_collection, left, right, onclause
            )
        else:
            # left is given via a relationship/name, or as explicit left side.
            # Determine where in our
            # "froms" list it should be spliced/appended as well as what
            # existing entity it corresponds to.
            (
                replace_from_obj_index,
                use_entity_index,
            ) = self._join_place_explicit_left_side(entities_collection, left)

        if left is right:
            raise sa_exc.InvalidRequestError(
                "Can't construct a join from %s to %s, they "
                "are the same entity" % (left, right)
            )

        # the right side as given often needs to be adapted.  additionally
        # a lot of things can be wrong with it.  handle all that and
        # get back the new effective "right" side

        if IS_VERSION_20:
            r_info, right, onclause = self._join_check_and_adapt_right_side(
                left, right, onclause, prop
            )
        else:
            r_info, right, onclause = self._join_check_and_adapt_right_side(
                left, right, onclause, prop, False, False
            )

        if not r_info.is_selectable:
            extra_criteria = self._get_extra_criteria(r_info)
        else:
            extra_criteria = ()

        if replace_from_obj_index is not None:
            # splice into an existing element in the
            # self._from_obj list
            left_clause = self.from_clauses[replace_from_obj_index]

            self.from_clauses = (
                self.from_clauses[:replace_from_obj_index]
                + [
                    _Snowflake_ORMJoin(  # handle Snowflake BCR bcr-1057
                        left_clause,
                        right,
                        onclause,
                        isouter=outerjoin,
                        full=full,
                        _extra_criteria=extra_criteria,
                    )
                ]
                + self.from_clauses[replace_from_obj_index + 1 :]
            )
        else:
            # add a new element to the self._from_obj list
            if use_entity_index is not None:
                # make use of _MapperEntity selectable, which is usually
                # entity_zero.selectable, but if with_polymorphic() were used
                # might be distinct
                assert isinstance(entities_collection[use_entity_index], _MapperEntity)
                left_clause = entities_collection[use_entity_index].selectable
            else:
                left_clause = left

            self.from_clauses = self.from_clauses + [
                _Snowflake_ORMJoin(  # handle Snowflake BCR bcr-1057
                    left_clause,
                    r_info,
                    onclause,
                    isouter=outerjoin,
                    full=full,
                    _extra_criteria=extra_criteria,
                )
            ]
