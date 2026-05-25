#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import itertools
import operator
import re
import string
import warnings
from functools import reduce
from typing import Any, List

from sqlalchemy import exc as sa_exc
from sqlalchemy import inspect, sql
from sqlalchemy import util as sa_util
from sqlalchemy.engine import default
from sqlalchemy.orm import context
from sqlalchemy.orm.context import _MapperEntity
from sqlalchemy.schema import Sequence, Table
from sqlalchemy.sql import compiler, expression, functions, sqltypes
from sqlalchemy.sql.base import CompileState
from sqlalchemy.sql.elements import BindParameter, quoted_name
from sqlalchemy.sql.expression import Executable
from sqlalchemy.sql.selectable import Lateral, SelectState

from snowflake.sqlalchemy._constants import DIALECT_NAME
from snowflake.sqlalchemy.custom_commands import (
    AWSBucket,
    AzureContainer,
    CloudStorageLocation,
    ExternalStage,
    GCSBucket,
)

from ._constants import NOT_NULL
from .exc import (
    CustomOptionsAreOnlySupportedOnSnowflakeTables,
    SnowflakeWarning,
    UnexpectedOptionTypeError,
)
from .functions import flatten
from .sql.custom_schema.custom_table_base import CustomTableBase
from .sql.custom_schema.options.table_option import TableOption
from .util import (
    _find_left_clause_to_join_from,
    _set_connection_interpolate_empty_sequences,
    _Snowflake_ORMJoin,
    _Snowflake_Selectable_Join,
    escape_backslashes,
    escape_string_literal_interior,
    requires_quotes,
    split_identifier_parts,
)

RESERVED_WORDS = frozenset(
    [
        "ALL",  # ANSI Reserved words
        "ALTER",
        "AND",
        "ANY",
        "AS",
        "BETWEEN",
        "BY",
        "CHECK",
        "COLUMN",
        "CONNECT",
        "COPY",
        "CREATE",
        "CURRENT",
        "DELETE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXISTS",
        "FOR",
        "FROM",
        "GRANT",
        "GROUP",
        "HAVING",
        "IN",
        "INSERT",
        "INTERSECT",
        "INTO",
        "IS",
        "LIKE",
        "NOT",
        "NULL",
        "OF",
        "ON",
        "OR",
        "ORDER",
        "REVOKE",
        "ROW",
        "ROWS",
        "SAMPLE",
        "SELECT",
        "SET",
        "START",
        "TABLE",
        "THEN",
        "TO",
        "TRIGGER",
        "UNION",
        "UNIQUE",
        "UPDATE",
        "VALUES",
        "WHENEVER",
        "WHERE",
        "WITH",
        "REGEXP",
        "RLIKE",
        "SOME",  # Snowflake Reserved words
        "MINUS",
        "INCREMENT",  # Oracle reserved words
    ]
)

# Snowflake DML:
# - UPDATE
# - INSERT
# - DELETE
# - MERGE
AUTOCOMMIT_REGEXP = re.compile(
    r"\s*(?:UPDATE|INSERT|DELETE|MERGE|COPY)", re.I | re.UNICODE
)
# used for quoting identifiers ie. table names, column names, etc.
ILLEGAL_INITIAL_CHARACTERS = frozenset({d for d in string.digits}.union({"$"}))


# used for quoting identifiers ie. table names, column names, etc.
ILLEGAL_IDENTIFIERS = frozenset({d for d in string.digits}.union({"_"}))

"""
Overwrite methods to handle Snowflake BCR change:
https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
- _join_determine_implicit_left_side
- _join_left_to_right
"""


# handle Snowflake BCR bcr-1057
@CompileState.plugin_for("default", "select")
class SnowflakeSelectState(SelectState):
    def __init__(self, statement, compiler, **kw):
        self._is_snowflake = (
            compiler is not None and compiler.dialect.name == DIALECT_NAME
        )
        super().__init__(statement, compiler, **kw)

    def _setup_joins(self, args, raw_columns):
        if not self._is_snowflake:
            return super()._setup_joins(args, raw_columns)
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
                replace_from_obj_index = self._join_place_explicit_left_side(left)

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

    @sa_util.preload_module("sqlalchemy.sql.util")
    def _join_determine_implicit_left_side(self, raw_columns, left, right, onclause):
        if not self._is_snowflake:
            return super()._join_determine_implicit_left_side(
                raw_columns, left, right, onclause
            )

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


# handle Snowflake BCR bcr-1057
@sql.base.CompileState.plugin_for("orm", "select")
class SnowflakeORMSelectCompileState(context.ORMSelectCompileState):
    # Set by _init_global_attributes (always called on SA 2.x).
    _is_snowflake = False

    def _init_global_attributes(self, statement, compiler, **kw):
        self._is_snowflake = (
            compiler is not None and compiler.dialect.name == DIALECT_NAME
        )
        super()._init_global_attributes(statement, compiler, **kw)

    def _join_determine_implicit_left_side(
        self, entities_collection, left, right, onclause
    ):
        if not self._is_snowflake:
            return super()._join_determine_implicit_left_side(
                entities_collection, left, right, onclause
            )

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

    def _join_left_to_right(
        self, entities_collection, left, right, onclause, prop, outerjoin, full
    ):
        if not self._is_snowflake:
            return super()._join_left_to_right(
                entities_collection, left, right, onclause, prop, outerjoin, full
            )

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

        r_info, right, onclause = self._join_check_and_adapt_right_side(
            left, right, onclause, prop
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


class SnowflakeIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {x.lower() for x in RESERVED_WORDS}
    illegal_initial_characters = ILLEGAL_INITIAL_CHARACTERS
    illegal_identifiers = ILLEGAL_IDENTIFIERS

    def __init__(self, dialect, **kw):
        quote = '"'

        super().__init__(dialect, initial_quote=quote, escape_quote=quote)

    def _safe_quote(self, ident):
        """Quote ``ident`` per dialect rules, but never emit an unsafe value raw.

        ``IdentifierPreparer.quote`` honours ``quoted_name(..., quote=False)``
        by returning the value verbatim.  In an identifier/schema position that
        is a quoting concern (SNOW-3649808): an application that wraps a
        user-derived identifier in ``quote=False`` would splat it unquoted into
        the compiled statement.  We override only that case — when quote=False
        but the value is *structurally* unsafe — while leaving legal bare
        identifiers (the documented quote=False idiom, including upper-case
        ones Snowflake folds) untouched.
        """
        if getattr(ident, "quote", None) is False and self._is_unsafe_unquoted(ident):
            return self.quote_identifier(ident)
        return self.quote(ident)

    @property
    def _identifier_cfg(self) -> dict:
        """Config bundle passed to the pure identifier predicates in util.py.

        Bundles the dialect-specific data the predicates need (reserved words,
        illegal-identifier sets, legal-character regex) so call sites stay terse.
        """
        return {
            "reserved_words": self.reserved_words,
            "illegal_identifiers": self.illegal_identifiers,
            "illegal_initial_characters": self.illegal_initial_characters,
            "legal_characters": self.legal_characters,
        }

    def _is_unsafe_unquoted(self, value: str) -> bool:
        """Return True if emitting ``value`` unquoted could alter SQL structure.

        Mirrors :meth:`_requires_quotes` but omits the case-only clause: an
        upper/mixed-case identifier is harmless emitted bare (Snowflake folds
        it), whereas whitespace, quotes, dots, parentheses, semicolons, etc. are
        what require quoting.  Used to neutralise risky values while still
        preserving the historical bare rendering of legal identifiers.

        Structural-only form of ``util.requires_quotes`` (``include_case=False``);
        the dialect config is supplied via ``_identifier_cfg``.
        """
        return requires_quotes(value, include_case=False, **self._identifier_cfg)

    def quote_identifier_if_unsafe(self, value: str) -> str:
        """Quote each dot-separated part of ``value`` only when it is unsafe.

        Used for custom-command identifiers (stage name/namespace, format_name,
        file_format) that historically render bare and must keep doing so for
        legal identifiers — including upper-case ones — while neutralising any
        part that contains SQL metacharacters (SNOW-3649881 / SNOW-3649858).
        """
        return ".".join(
            self.quote_identifier(p) if self._is_unsafe_unquoted(p) else str(p)
            for p in self._split_schema_by_dot(value)
            if p is not None
        )

    def _quote_free_identifiers(self, *ids):
        """
        Identifier-quote any number of strings, quoting whenever the value
        requires it.  Unlike a bare ``quote()`` this refuses to emit an unsafe
        ``quote=False`` identifier verbatim (see :meth:`_safe_quote`).
        """
        return tuple(self._safe_quote(i) for i in ids if i is not None)

    def quote_schema(self, schema, force=None):
        """
        Split schema by a dot and merge with required quotes
        """
        # SA 2.0 schema-translate tokens arrive as
        # ``quoted_name("__[SCHEMA_<key>]", quote=False)``.  _safe_quote
        # (used inside _quote_free_identifiers) overrides quote=False when
        # the value contains unsafe characters — but the bracket characters
        # in SA's internal token are safe as-is; quoting them
        # destroys the token and breaks SA's post-compile substitution.
        # Return the token string as-is so SA can resolve it normally.
        if getattr(schema, "quote", None) is False and str(schema).startswith(
            "__[SCHEMA_"
        ):
            return str(schema)
        idents = self._split_schema_by_dot(schema)
        return ".".join(self._quote_free_identifiers(*idents))

    def format_label(self, label, name=None):
        n = name or label.name
        s = n.replace(self.escape_quote, "")

        if not isinstance(n, quoted_name) or n.quote is None:
            return self.quote(s)
        if n.quote:
            return self.quote_identifier(s)
        # n.quote is False: previously returned ``s`` verbatim, which let an
        # application that wrapped a user-supplied alias in
        # ``quoted_name(alias, quote=False)`` could emit arbitrary SQL into the
        # projection list (SNOW-3649824).  _safe_quote encodes the rule:
        # honour quote=False for legal bare identifiers, force-quote if unsafe.
        return self._safe_quote(s)

    def _requires_quotes(self, value: str) -> bool:
        """Return True if the given identifier requires quoting.

        Thin wrapper over ``util.requires_quotes`` (structural triggers plus the
        case-only clause).
        """
        return requires_quotes(value, **self._identifier_cfg)

    def _split_schema_by_dot(self, schema):
        # Scan the raw string into ``(value, was_quoted)`` parts; the pure
        # scanner lives in util.split_identifier_parts so it can be unit-tested
        # without a preparer.
        ret = split_identifier_parts(schema)

        # Parts found inside "..." get ``quote=True`` only when the dialect
        # was constructed with ``case_sensitive_identifiers=True``.  Without
        # that opt-in we fall back to the input schema's ``.quote`` attribute
        # (``None`` for a plain str) so the preparer's ``_requires_quotes``
        # heuristic keeps its pre-existing behaviour — avoids a silent BCR
        # for users who pass ``'"myschema"'`` and previously saw the inner
        # quotes stripped by the heuristic.
        schema_quote = getattr(schema, "quote", None)
        case_sensitive = getattr(self.dialect, "_case_sensitive_identifiers", False)
        return [
            quoted_name(
                value,
                quote=True if (was_quoted and case_sensitive) else schema_quote,
            )
            for value, was_quoted in ret
        ]

    def _split_idents(self, *idents) -> list:
        """Split each non-None identifier on its unquoted dots and concatenate
        the parts; the all-None / empty case yields ``[]``."""
        return reduce(
            operator.add,
            [self._split_schema_by_dot(i) for i in idents if i is not None],
            [],
        )


def _render_storage_credentials(credentials_used, deterministic: bool = False) -> str:
    """Render a ``CREDENTIALS=(...)`` clause with escaped literal values.

    Credential values (SAS tokens, secret keys, KMS ids, ...) are
    caller-supplied and embedded in single-quoted literals, so each is escaped
    to neutralise single-quote / backslash sequences (SNOW-3649816).  Keys come
    from the closed set defined by the bucket helpers and are emitted as-is.
    """
    items = list(credentials_used.items())
    if deterministic:
        items.sort(key=operator.itemgetter(0))
    return "CREDENTIALS=({})".format(
        " ".join(f"{n}='{escape_string_literal_interior(str(v))}'" for n, v in items)
    )


def _render_storage_encryption(encryption_used, deterministic: bool = False) -> str:
    """Render an ``ENCRYPTION=(...)`` clause with escaped literal values."""
    items = list(encryption_used.items())
    if deterministic:
        items.sort(key=operator.itemgetter(0))
    return "ENCRYPTION=({})".format(
        " ".join(
            (
                f"{n}='{escape_string_literal_interior(str(v))}'"
                if isinstance(v, str)
                else f"{n}={v}"
            )
            for n, v in items
        )
    )


def _render_storage_uri(container) -> str:
    """Return the escaped, single-quoted storage location literal for a container.

    The bucket/path/account components originate from caller-supplied URIs
    (``*.from_uri``), so the assembled body is escaped before being wrapped in
    quotes — a single-quote in any component would otherwise escape the
    location literal (SNOW-3649816 / SNOW-3649858).
    """
    if isinstance(container, AWSBucket):
        body = "s3://{}{}".format(
            container.bucket, f"/{container.path}" if container.path else ""
        )
    elif isinstance(container, AzureContainer):
        body = "azure://{}.blob.core.windows.net/{}{}".format(
            container.account,
            container.container,
            f"/{container.path}" if container.path else "",
        )
    elif isinstance(container, GCSBucket):
        body = "gcs://{}{}".format(
            container.bucket, f"/{container.path}" if container.path else ""
        )
    else:
        raise TypeError(
            f"Unsupported cloud storage location: {type(container).__name__}"
        )
    return f"'{escape_string_literal_interior(body)}'"


class SnowflakeCompiler(compiler.SQLCompiler):
    def visit_sequence(self, sequence, **kw):
        return self.dialect.identifier_preparer.format_sequence(sequence) + ".nextval"

    def visit_now_func(self, now, **kw):
        return "CURRENT_TIMESTAMP"

    def visit_sysdate_func(self, sysdate, **kw):
        return "SYSDATE()"

    def visit_merge_into(self, merge_into, **kw):
        clauses = " ".join(
            clause._compiler_dispatch(self, **kw) for clause in merge_into.clauses
        )
        target = merge_into.target._compiler_dispatch(self, asfrom=True, **kw)
        source = merge_into.source._compiler_dispatch(self, asfrom=True, **kw)
        on = merge_into.on._compiler_dispatch(self, **kw)
        return f"MERGE INTO {target} USING {source} ON {on}" + (
            " " + clauses if clauses else ""
        )

    def visit_merge_into_clause(self, merge_into_clause, **kw):
        case_predicate = (
            f" AND {str(merge_into_clause.predicate._compiler_dispatch(self, **kw))}"
            if merge_into_clause.predicate is not None
            else ""
        )
        if merge_into_clause.command == "INSERT":
            sets, sets_tos = zip(*merge_into_clause.set.items())
            sets, sets_tos = list(sets), list(sets_tos)
            if kw.get("deterministic", False):
                sets, sets_tos = zip(
                    *sorted(merge_into_clause.set.items(), key=operator.itemgetter(0))
                )
            return "WHEN NOT MATCHED{} THEN {} ({}) VALUES ({})".format(
                case_predicate,
                merge_into_clause.command,
                # Column keys come straight from clause.values(**kwargs) with no
                # resolution against the target table, so an application driving
                # the column set from external input could emit unintended SQL here
                # (SNOW-3649763).  Identifier-quote each key.
                ", ".join(self.preparer.quote(s) for s in sets),
                ", ".join(map(lambda e: e._compiler_dispatch(self, **kw), sets_tos)),
            )
        else:
            set_list = list(merge_into_clause.set.items())
            if kw.get("deterministic", False):
                set_list.sort(key=operator.itemgetter(0))
            sets = (
                ", ".join(
                    [
                        # Same untrusted-key source as the INSERT branch above
                        # (SNOW-3649763); quote the assignment target.
                        f"{self.preparer.quote(set[0])} = "
                        f"{set[1]._compiler_dispatch(self, **kw)}"
                        for set in set_list
                    ]
                )
                if merge_into_clause.set
                else ""
            )
            return "WHEN MATCHED{} THEN {}{}".format(
                case_predicate,
                merge_into_clause.command,
                " SET %s" % sets if merge_into_clause.set else "",
            )

    def visit_copy_into(self, copy_into, **kw):
        if hasattr(copy_into, "formatter") and copy_into.formatter is not None:
            formatter = copy_into.formatter._compiler_dispatch(self, **kw)
        else:
            formatter = ""
        into = copy_into.into._compiler_dispatch(self, asfrom=True, **kw)
        from_ = None
        if isinstance(copy_into.from_, Table):
            from_ = copy_into.from_.name
        elif isinstance(copy_into.from_, (CloudStorageLocation, ExternalStage)):
            from_ = copy_into.from_._compiler_dispatch(self, **kw)
        # everything else (selects, etc.)
        else:
            from_ = f"({copy_into.from_._compiler_dispatch(self, **kw)})"

        partition_by_value = None
        if isinstance(copy_into.partition_by, (BindParameter, Executable)):
            partition_by_value = copy_into.partition_by.compile(
                compile_kwargs={"literal_binds": True}
            )
        elif copy_into.partition_by is not None:
            partition_by_value = copy_into.partition_by

        partition_by = (
            f"PARTITION BY {partition_by_value}"
            if partition_by_value is not None and partition_by_value != ""
            else ""
        )

        credentials, encryption = "", ""
        if isinstance(into, tuple):
            into, credentials, encryption = into
        elif isinstance(from_, tuple):
            from_, credentials, encryption = from_
        options_list = list(copy_into.copy_options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        options = (
            (
                " ".join(
                    [
                        "{} = {}".format(
                            n,
                            (
                                v._compiler_dispatch(self, **kw)
                                if getattr(v, "compiler_dispatch", False)
                                else str(v)
                            ),
                        )
                        for n, v in options_list
                    ]
                )
            )
            if copy_into.copy_options
            else ""
        )
        if credentials:
            options += f" {credentials}"
        if encryption:
            options += f" {encryption}"
        return f"COPY INTO {into} FROM {' '.join([from_, partition_by, formatter, options])}"

    def visit_copy_formatter(self, formatter, **kw):
        options_list = list(formatter.options.items())
        if kw.get("deterministic", False):
            options_list.sort(key=operator.itemgetter(0))
        if "format_name" in formatter.options:
            # format_name is a (possibly schema-qualified) identifier supplied
            # by the caller; quote any unsafe part so it cannot alter the COPY
            # options / SQL while keeping normal names bare (SNOW-3649881).
            format_name = self.preparer.quote_identifier_if_unsafe(
                formatter.options["format_name"]
            )
            return f"FILE_FORMAT=(format_name = {format_name})"
        return "FILE_FORMAT=(TYPE={}{})".format(
            formatter.file_format,
            (
                " "
                + " ".join(
                    [
                        "{}={}".format(
                            name,
                            (
                                value._compiler_dispatch(self, **kw)
                                if hasattr(value, "_compiler_dispatch")
                                else formatter.value_repr(name, value)
                            ),
                        )
                        for name, value in options_list
                    ]
                )
                if formatter.options
                else ""
            ),
        )

    def visit_aws_bucket(self, aws_bucket, **kw):
        deterministic = kw.get("deterministic", False)
        return (
            _render_storage_uri(aws_bucket),
            (
                _render_storage_credentials(aws_bucket.credentials_used, deterministic)
                if aws_bucket.credentials_used
                else ""
            ),
            (
                _render_storage_encryption(aws_bucket.encryption_used, deterministic)
                if aws_bucket.encryption_used
                else ""
            ),
        )

    def visit_azure_container(self, azure_container, **kw):
        deterministic = kw.get("deterministic", False)
        return (
            _render_storage_uri(azure_container),
            (
                _render_storage_credentials(
                    azure_container.credentials_used, deterministic
                )
                if azure_container.credentials_used
                else ""
            ),
            (
                _render_storage_encryption(
                    azure_container.encryption_used, deterministic
                )
                if azure_container.encryption_used
                else ""
            ),
        )

    def visit_gcs_bucket(self, gcs_bucket, **kw):
        deterministic = kw.get("deterministic", False)
        return (
            _render_storage_uri(gcs_bucket),
            "",
            (
                _render_storage_encryption(gcs_bucket.encryption_used, deterministic)
                if gcs_bucket.encryption_used
                else ""
            ),
        )

    def visit_external_stage(self, external_stage, **kw):
        # Quote the stage's <namespace>.<name> prefix when required, consistently
        # with CREATE STAGE, so the stage reference is always a well-formed
        # identifier; the trailing path is a stage path, not an identifier.
        prefix = self.preparer.quote_identifier_if_unsafe(
            f"{external_stage.namespace}{external_stage.name}"
        )
        if external_stage.file_format is None:
            return f"@{prefix}{external_stage.path}"
        # file_format names a (possibly schema-qualified) file format object;
        # quote any part that requires it so the stage reference stays
        # well-formed.
        file_format = self.preparer.quote_identifier_if_unsafe(
            external_stage.file_format
        )
        return f"@{prefix}{external_stage.path} (file_format => {file_format})"

    def delete_extra_from_clause(
        self, delete_stmt, from_table, extra_froms, from_hints, **kw
    ):
        return "USING " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    def update_from_clause(
        self, update_stmt, from_table, extra_froms, from_hints, **kw
    ):
        return "FROM " + ", ".join(
            t._compiler_dispatch(self, asfrom=True, fromhints=from_hints, **kw)
            for t in extra_froms
        )

    def _get_regexp_args(self, binary, kw):
        string = self.process(binary.left, **kw)
        pattern = self.process(binary.right, **kw)
        flags = binary.modifiers["flags"]
        if flags is not None:
            flags = self.render_literal_value(flags, sqltypes.STRINGTYPE)
        return string, pattern, flags

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        if flags is None:
            return f"REGEXP_LIKE({string}, {pattern})"
        else:
            return f"REGEXP_LIKE({string}, {pattern}, {flags})"

    def visit_regexp_replace_op_binary(self, binary, operator, **kw):
        string, pattern, flags = self._get_regexp_args(binary, kw)
        try:
            replacement = self.process(binary.modifiers["replacement"], **kw)
        except KeyError:
            # in sqlalchemy 1.4.49, the internal structure of the expression is changed
            # that binary.modifiers doesn't have "replacement":
            # https://docs.sqlalchemy.org/en/20/changelog/changelog_14.html#change-1.4.49
            return f"REGEXP_REPLACE({string}, {pattern}{'' if flags is None else f', {flags}'})"

        if flags is None:
            return f"REGEXP_REPLACE({string}, {pattern}, {replacement})"
        else:
            return f"REGEXP_REPLACE({string}, {pattern}, {replacement}, {flags})"

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return f"NOT {self.visit_regexp_match_op_binary(binary, operator, **kw)}"

    def visit_ilike_op_binary(self, binary, operator, **kw):
        return self._render_ilike(binary, negate=False, **kw)

    def visit_not_ilike_op_binary(self, binary, operator, **kw):
        return self._render_ilike(binary, negate=True, **kw)

    def _render_ilike(self, binary, negate=False, **kw):
        left = binary.left._compiler_dispatch(self, **kw)
        right = binary.right._compiler_dispatch(self, **kw)
        escape = binary.modifiers.get("escape")
        escape_clause = (
            " ESCAPE " + self.render_literal_value(escape, sqltypes.STRINGTYPE)
            if escape is not None
            else ""
        )
        operator = "NOT ILIKE" if negate else "ILIKE"
        return f"{left} {operator} {right}{escape_clause}"

    def visit_join(self, join, asfrom=False, from_linter=None, **kwargs):
        if from_linter:
            from_linter.edges.update(
                itertools.product(join.left._from_objects, join.right._from_objects)
            )

        if join.full:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        else:
            join_type = " JOIN "

        join_statement = (
            join.left._compiler_dispatch(
                self, asfrom=True, from_linter=from_linter, **kwargs
            )
            + join_type
            + join.right._compiler_dispatch(
                self, asfrom=True, from_linter=from_linter, **kwargs
            )
        )

        if join.onclause is None and isinstance(join.right, Lateral):
            # in snowflake, onclause is not accepted for lateral due to BCR change:
            # https://docs.snowflake.com/en/release-notes/bcr-bundles/2023_04/bcr-1057
            # sqlalchemy only allows join with on condition.
            # to adapt to snowflake syntax change,
            # we make the change such that when oncaluse is None and the right part is
            # Lateral, we do not append the on condition
            return join_statement

        return (
            join_statement
            + " ON "
            # TODO: likely need asfrom=True here?
            + join.onclause._compiler_dispatch(self, from_linter=from_linter, **kwargs)
        )

    def visit_truediv_binary(self, binary, operator, **kw):
        if self.dialect.div_is_floordiv:
            warnings.warn(
                "div_is_floordiv value will be changed to False in a future release. This will generate a behavior change on true and floor division. Please review https://docs.sqlalchemy.org/en/20/changelog/whatsnew_20.html#python-division-operator-performs-true-division-for-all-backends-added-floor-division",
                PendingDeprecationWarning,
                stacklevel=2,
            )
            return super().visit_truediv_binary(binary, operator, **kw)
        return (
            self.process(binary.left, **kw) + " / " + self.process(binary.right, **kw)
        )

    def visit_floordiv_binary(self, binary, operator, **kw):
        if self.dialect.div_is_floordiv:
            warnings.warn(
                "div_is_floordiv value will be changed to False in a future release. This will generate a behavior change on true and floor division. Please review https://docs.sqlalchemy.org/en/20/changelog/whatsnew_20.html#python-division-operator-performs-true-division-for-all-backends-added-floor-division",
                PendingDeprecationWarning,
                stacklevel=2,
            )
        return super().visit_floordiv_binary(binary, operator, **kw)

    def render_literal_value(self, value, type_):
        # escape backslash
        return escape_backslashes(super().render_literal_value(value, type_))


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


# Tracks (table_name, column_name) pairs for which the Identity-on-PK warning
# has already been emitted this session, preventing duplicate output when the
# same table schema is compiled repeatedly (e.g. inside ORM session loops).
_identity_pk_warned: set = set()


class SnowflakeDDLCompiler(compiler.DDLCompiler):
    def denormalize_column_name(self, name):
        if name is None:
            return None
        if isinstance(name, quoted_name) and name.quote is True:
            # Caller explicitly requested quoting — preserve case by quoting.
            return self.preparer.quote_identifier(name)
        elif name.lower() == name and not self.preparer._requires_quotes(name.lower()):
            # no quote as case insensitive
            return name
        return self.preparer.quote(name)

    def get_column_specification(self, column, **kwargs):
        """
        Gets Column specifications
        """
        colspec = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(column.type, type_expression=column),
        ]

        has_identity = (
            column.identity is not None and self.dialect.supports_identity_columns
        )

        if not column.nullable:
            colspec.append("NOT NULL")

        default = self.get_column_default_string(column)
        if default is not None:
            colspec.append("DEFAULT " + default)

        # TODO: This makes the first INTEGER column AUTOINCREMENT.
        # But the column is not really considered so unless
        # postfetch_lastrowid is enabled. But it is very unlikely to happen...
        if (
            column.table is not None
            and column is column.table._autoincrement_column
            and column.server_default is None
        ):
            if isinstance(column.default, Sequence):
                colspec.append(
                    f"DEFAULT {self.dialect.identifier_preparer.format_sequence(column.default)}.nextval"
                )
            else:
                colspec.append("AUTOINCREMENT")

        if has_identity:
            if column.primary_key:
                key = (column.table.name, column.name)
                if key not in _identity_pk_warned:
                    _identity_pk_warned.add(key)
                    warnings.warn(
                        f"Column '{column.name}' uses Identity() as a primary key. "
                        "Snowflake does not support retrieving the last inserted identity "
                        "value via the Python connector, which will cause a FlushError "
                        "during SQLAlchemy ORM flush operations. "
                        f"Use Sequence() instead: Column('{column.name}', Integer, "
                        "Sequence('seq_name'), primary_key=True)",
                        SnowflakeWarning,
                        stacklevel=2,
                    )
            colspec.append(self.process(column.identity))

        return " ".join(colspec)

    def handle_cluster_by(self, table):
        """
        Handles snowflake-specific ``CREATE TABLE ... CLUSTER BY`` syntax.

        Users can specify the `clusterby` property per table
        using the dialect specific syntax.
        For example, to specify a cluster by key you apply the following:

        >>> import sqlalchemy as sa
        >>> from sqlalchemy.schema import CreateTable
        >>> engine = sa.create_engine('snowflake://om1')
        >>> metadata = sa.MetaData()
        >>> user = sa.Table(
        ...     'user',
        ...     metadata,
        ...     sa.Column('id', sa.Integer, primary_key=True),
        ...     sa.Column('name', sa.String),
        ...     snowflake_clusterby=['id', 'name', text("id > 5")]
        ... )
        >>> print(CreateTable(user).compile(engine))
        <BLANKLINE>
        CREATE TABLE "user" (
            id INTEGER NOT NULL AUTOINCREMENT,
            name VARCHAR,
            PRIMARY KEY (id)
        ) CLUSTER BY (id, name, id > 5)
        <BLANKLINE>
        <BLANKLINE>
        """
        text = ""
        info = table.dialect_options[DIALECT_NAME]
        cluster = info.get("clusterby")
        if cluster:
            text += " CLUSTER BY ({})".format(
                ", ".join(
                    (
                        self.denormalize_column_name(key)
                        if isinstance(key, str)
                        else str(key)
                    )
                    for key in cluster
                )
            )
        return text

    def post_create_table(self, table):
        text = self.handle_cluster_by(table)
        options = []
        invalid_options: List[str] = []

        for key, option in table.dialect_options[DIALECT_NAME].items():
            if isinstance(option, TableOption):
                options.append(option)
            elif key not in ["clusterby", "*"]:
                invalid_options.append(key)

        if len(invalid_options) > 0:
            raise UnexpectedOptionTypeError(sorted(invalid_options))

        if isinstance(table, CustomTableBase):
            options.sort(key=lambda x: (x.priority.value, x.option_name), reverse=True)
            for option in options:
                text += "\t" + option.render_option(self)
        elif len(options) > 0:
            raise CustomOptionsAreOnlySupportedOnSnowflakeTables()

        return text

    def _format_stage_prefix(self, stage) -> str:
        """Identifier-quote a stage's ``<namespace>.<name>`` prefix.

        ``ExternalStage.prepare_namespace`` stores the namespace with a trailing
        dot (or ``""``), so ``f"{stage.namespace}{stage.name}"`` always yields a
        clean dotted identifier (``"DB.SCH.S"`` or ``"S"``).
        ``quote_identifier_if_unsafe`` splits on dots internally, so the combined
        string can be passed directly — no manual strip/rejoin needed.
        Legal identifiers stay bare (no BCR).  (SNOW-3649858)
        """
        return self.preparer.quote_identifier_if_unsafe(
            f"{stage.namespace}{stage.name}"
        )

    def visit_create_stage(self, create_stage, **kw):
        """
        This visitor will create the SQL representation for a CREATE STAGE command.
        """
        # Render the storage URL/credentials/encryption through the shared,
        # escaped helpers rather than repr(container): repr() is a debug
        # representation (and is redacted separately to hide secrets),
        # and using it as a SQL serialiser left single-quote escaping incomplete
        # (SNOW-3649858).
        container = create_stage.container
        deterministic = kw.get("deterministic", False)
        uri = _render_storage_uri(container)
        credentials = (
            _render_storage_credentials(container.credentials_used, deterministic)
            if getattr(container, "credentials_used", None)
            else ""
        )
        encryption = (
            _render_storage_encryption(container.encryption_used, deterministic)
            if getattr(container, "encryption_used", None)
            else ""
        )
        storage = "URL={}{}{}".format(
            uri,
            f" {credentials}" if credentials else "",
            f" {encryption}" if encryption else "",
        )
        return "CREATE {or_replace}{temporary}STAGE {prefix} {storage}".format(
            or_replace="OR REPLACE " if create_stage.replace_if_exists else "",
            temporary="TEMPORARY " if create_stage.temporary else "",
            prefix=self._format_stage_prefix(create_stage.stage),
            storage=storage,
        )

    def visit_create_file_format(self, file_format, **kw):
        """
        This visitor will create the SQL representation for a CREATE FILE FORMAT
        command.
        """
        return "CREATE {}FILE FORMAT {} TYPE='{}' {}".format(
            "OR REPLACE " if file_format.replace_if_exists else "",
            # format_name is a (possibly schema-qualified) identifier; quote any
            # unsafe part to prevent unintended DDL (SNOW-3649881).
            self.preparer.quote_identifier_if_unsafe(file_format.format_name),
            file_format.formatter.file_format,
            " ".join(
                [
                    f"{name} = {file_format.formatter.value_repr(name, value)}"
                    for name, value in file_format.formatter.options.items()
                ]
            ),
        )

    def visit_drop_table_comment(self, drop, **kw):
        """Snowflake does not support setting table comments as NULL.

        Reflection has to account for this and convert any empty comments to NULL.
        """
        table_name = self.preparer.format_table(drop.element)
        return f"COMMENT ON TABLE {table_name} IS ''"

    def visit_drop_column_comment(self, drop, **kw):
        """Snowflake does not support directly setting column comments as NULL.

        Instead we are forced to use the ALTER COLUMN ... UNSET COMMENT instead.
        """
        return "ALTER TABLE {} ALTER COLUMN {} UNSET COMMENT".format(
            self.preparer.format_table(drop.element.table),
            self.preparer.format_column(drop.element),
        )

    def visit_identity_column(self, identity, **kw):
        text = "IDENTITY"
        if identity.start is not None or identity.increment is not None:
            start = 1 if identity.start is None else identity.start
            increment = 1 if identity.increment is None else identity.increment
            text += f"({start},{increment})"
        if identity.order is not None:
            order = "ORDER" if identity.order else "NOORDER"
            text += f" {order}"
        return text

    def get_identity_options(self, identity_options):
        text = []
        if identity_options.increment is not None:
            text.append("INCREMENT BY %d" % identity_options.increment)
        if identity_options.start is not None:
            text.append("START WITH %d" % identity_options.start)
        if identity_options.minvalue is not None:
            text.append("MINVALUE %d" % identity_options.minvalue)
        if identity_options.maxvalue is not None:
            text.append("MAXVALUE %d" % identity_options.maxvalue)
        if identity_options.nominvalue is not None:
            text.append("NO MINVALUE")
        if identity_options.nomaxvalue is not None:
            text.append("NO MAXVALUE")
        if identity_options.cache is not None:
            text.append("CACHE %d" % identity_options.cache)
        if identity_options.cycle is not None:
            text.append("CYCLE" if identity_options.cycle else "NO CYCLE")
        if identity_options.order is not None:
            text.append("ORDER" if identity_options.order else "NOORDER")
        return " ".join(text)


class SnowflakeTypeCompiler(compiler.GenericTypeCompiler):
    def visit_BYTEINT(self, type_, **kw):
        return "BYTEINT"

    def visit_CHARACTER(self, type_, **kw):
        return "CHARACTER"

    def visit_DEC(self, type_, **kw):
        return "DEC"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_FIXED(self, type_, **kw):
        return "FIXED"

    def visit_INT(self, type_, **kw):
        return "INT"

    def visit_NUMBER(self, type_, **kw):
        return "NUMBER"

    def visit_STRING(self, type_, **kw):
        return "STRING"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_VARIANT(self, type_, **kw):
        return "VARIANT"

    def visit_MAP(self, type_, **kw):
        not_null = f" {NOT_NULL}" if type_.not_null else ""
        return (
            f"MAP({type_.key_type.compile()}, {type_.value_type.compile()}{not_null})"
        )

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY"

    def visit_SNOWFLAKE_ARRAY(self, type_, **kw):
        if type_.is_semi_structured:
            return "ARRAY"
        not_null = f" {NOT_NULL}" if type_.not_null else ""
        return f"ARRAY({type_.value_type.compile()}{not_null})"

    def visit_OBJECT(self, type_, **kw):
        if type_.is_semi_structured:
            return "OBJECT"
        else:
            contents = []
            ip = self.dialect.identifier_preparer
            for key in type_.items_types:
                if len(key) >= 2 and key[0] == '"' and key[-1] == '"':
                    inner = key[1:-1].replace('""', '"')
                else:
                    inner = key
                quoted_key = ip.quote(inner)
                row_text = f"{quoted_key} {type_.items_types[key][0].compile()}"
                # Type and not null is specified
                if len(type_.items_types[key]) > 1:
                    row_text += f"{' NOT NULL' if type_.items_types[key][1] else ''}"
                contents.append(row_text)
            return "OBJECT" if contents == [] else f"OBJECT({', '.join(contents)})"

    def visit_BLOB(self, type_, **kw):
        return "BINARY"

    def visit_datetime(self, type_: sqltypes.DateTime, **kw: Any) -> str:
        if type_.timezone:
            return "TIMESTAMP_TZ"
        return "datetime"

    def visit_DATETIME(self, type_: sqltypes.DateTime, **kw: Any) -> str:
        if type_.timezone:
            return "TIMESTAMP_TZ"
        return "DATETIME"

    def visit_TIMESTAMP_NTZ(self, type_, **kw):
        return "TIMESTAMP_NTZ"

    def visit_TIMESTAMP_TZ(self, type_, **kw):
        return "TIMESTAMP_TZ"

    def visit_TIMESTAMP_LTZ(self, type_, **kw):
        return "TIMESTAMP_LTZ"

    def visit_TIMESTAMP(self, type_: sqltypes.TIMESTAMP, **kw: Any) -> str:
        if type_.timezone:
            return "TIMESTAMP_TZ"
        return "TIMESTAMP"

    def visit_GEOGRAPHY(self, type_, **kw):
        return "GEOGRAPHY"

    def visit_GEOMETRY(self, type_, **kw):
        return "GEOMETRY"

    def visit_DECFLOAT(self, type_, **kw):
        return "DECFLOAT"

    def visit_VECTOR(self, type_, **kw):
        return f"VECTOR({type_.element_type}, {type_.dimension})"


construct_arguments = [(Table, {"clusterby": None})]

functions.register_function("flatten", flatten, "snowflake")
