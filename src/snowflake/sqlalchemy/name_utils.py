#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import operator
from functools import reduce

from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.sql.elements import quoted_name


class _NameUtils:

    def __init__(
        self,
        identifier_preparer: IdentifierPreparer,
        case_sensitive_identifiers: bool = False,
    ) -> None:
        self.identifier_preparer = identifier_preparer
        self.case_sensitive_identifiers = case_sensitive_identifiers

    def normalize_name(self, name):
        if name is None:
            return None
        if name == "":
            return ""
        if name.upper() == name:
            lc = name.lower()
            if not self.identifier_preparer._requires_quotes(lc):
                # Plain ASCII-uppercase identifier (e.g. MYTABLE) → lowercase
                return lc
            elif self.case_sensitive_identifiers:
                # Reserved-word ALL-UPPERCASE (e.g. TABLE) with flag on:
                # return as case-sensitive quoted_name so the ORM stores it
                # under the lowercase key rather than the uppercase original.
                return quoted_name(lc, quote=True)
            else:
                # Legacy: reserved-word ALL-UPPERCASE falls through unchanged.
                return name
        elif name.lower() == name:
            return quoted_name(name, quote=True)
        elif self.case_sensitive_identifiers:
            # Opt-in: mixed-case names (e.g. "MyTable") can only exist in
            # Snowflake when the identifier was SQL-quoted at creation time.
            # Marking them quote=True makes the case-sensitivity signal
            # explicit so MetaData.tables keyed lookups stay consistent with
            # emitted SQL and with tools that inspect .quote (e.g. Alembic
            # render_item).
            return quoted_name(name, quote=True)
        else:
            # Legacy (default): return mixed-case as a plain str.  The
            # preparer's _requires_quotes heuristic forces double-quoting at
            # SQL-render time because the name contains uppercase chars, so
            # emitted SQL is unchanged from the flag-on branch — the only
            # observable difference is the Python type and .quote attribute.
            return name

    def denormalize_name(self, name):
        if name is None:
            return None
        if name == "":
            return ""
        elif name.lower() == name and not self.identifier_preparer._requires_quotes(
            name.lower()
        ):
            name = name.upper()
        return name

    def _quote_component(self, component) -> str:
        """Unconditionally double-quote a single pre-split identifier component.

        Components marked ``quote=True`` are taken verbatim (case preserved);
        others are denormalized first so a plain lowercase name maps to the
        Snowflake-stored uppercase form.

        Use this only for parts that were already extracted by
        ``_split_schema_by_dot`` — do NOT call it on dotted strings because
        it will not split them first.
        """
        ip = self.identifier_preparer
        name = str(component)
        if getattr(component, "quote", None):
            return ip.quote_identifier(name)
        return ip.quote_identifier(self.denormalize_name(name))

    def always_quote_join(self, *idents) -> str:
        """Build a dot-joined SQL identifier string that always quotes every part.

        Each identifier in *idents is split on unquoted dots via
        ``_split_schema_by_dot`` (so ``"db.schema"`` becomes two components),
        then every component is unconditionally double-quoted via
        ``_quote_component``.

        Do NOT pass pre-split parts that may contain literal dots (e.g. a
        component extracted from ``'"my.schema"'``).  Use ``_quote_component``
        directly on each pre-split part instead.
        """
        split_idents = reduce(
            operator.add,
            [
                self.identifier_preparer._split_schema_by_dot(ids)
                for ids in idents
                if ids is not None
            ],
        )
        return ".".join(self._quote_component(i) for i in split_idents)
