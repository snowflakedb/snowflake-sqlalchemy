#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Lightweight fully-qualified-name (FQN) parsing for Snowflake identifiers.

The identifier regexes and the parsing approach are adapted from Snowflake CLI
(``snowflake.cli.api.identifiers`` / ``snowflake.cli.api.project.util``), which
is licensed under the Apache License 2.0 -- the same license as this project.
Only the minimal subset needed to split and render ``[[db.]schema.]name`` is
kept here.

Tokenization is delegated to :func:`snowflake.sqlalchemy.util.split_identifier_parts`
-- the same scanner the dialect's ``SnowflakeIdentifierPreparer`` uses -- so the
part/quoting model is identical everywhere: each part is stored as a
``quoted_name`` carrying the *unquoted* value plus a ``quote`` flag. The regex
below is used only as a strict well-formedness gate (rejects empty parts, more
than three parts, unquoted names with illegal characters, etc.); the scanner
then produces the actual parts. Rendering to SQL is the caller's job (via the
preparer), which avoids the previous mismatch where FQN kept surrounding quotes
and the preparer re-quoted them (a quoted ``Sec`` became triple-quoted).
"""

from __future__ import annotations

import re

from sqlalchemy.sql.elements import quoted_name

from .util import split_identifier_parts

# See https://docs.snowflake.com/en/sql-reference/identifiers-syntax
UNQUOTED_IDENTIFIER_REGEX = r"[A-Za-z_][A-Za-z0-9_$]{0,254}"
QUOTED_IDENTIFIER_REGEX = r'"(?:""|[^"]){0,255}"'
VALID_IDENTIFIER_REGEX = f"(?:{UNQUOTED_IDENTIFIER_REGEX}|{QUOTED_IDENTIFIER_REGEX})"

# One optional database qualifier, one optional schema qualifier, then the name.
_FQN_REGEX = re.compile(
    rf"(?:(?P<first>{VALID_IDENTIFIER_REGEX})\.)?"
    rf"(?:(?P<second>{VALID_IDENTIFIER_REGEX})\.)?"
    rf"(?P<name>{VALID_IDENTIFIER_REGEX})"
)


class FQN:
    """A Snowflake object name, optionally qualified by database and schema.

    Parse with :meth:`from_string`. Each part is a ``quoted_name`` holding the
    *unquoted* value with a ``quote`` flag (``True`` when the source token was
    double-quoted). This matches the dialect preparer's part model, so parts can
    be handed to the preparer for per-part quoting without double-quoting.
    """

    def __init__(
        self,
        name: str,
        schema: str | None = None,
        database: str | None = None,
    ) -> None:
        self._name = name
        self._schema = schema
        self._database = database

    @property
    def name(self) -> str:
        return self._name

    @property
    def schema(self) -> str | None:
        return self._schema

    @property
    def database(self) -> str | None:
        return self._database

    @property
    def parts(self) -> list[str]:
        """The present parts, ordered ``[database?, schema?, name]``."""
        return [p for p in (self._database, self._schema, self._name) if p is not None]

    @staticmethod
    def _render_part(part: str) -> str:
        if getattr(part, "quote", None):
            return '"' + str(part).replace('"', '""') + '"'
        return str(part)

    @property
    def identifier(self) -> str:
        """Canonical dotted form, re-quoting parts that were quoted at parse time."""
        return ".".join(self._render_part(p) for p in self.parts)

    @classmethod
    def from_string(cls, identifier: str) -> FQN:
        """Parse ``[[database.]schema.]name`` into an :class:`FQN`.

        Raises ``ValueError`` when ``identifier`` is not a well-formed
        (optionally qualified) Snowflake identifier.
        """
        if re.fullmatch(_FQN_REGEX, identifier) is None:
            raise ValueError(f"Invalid Snowflake identifier: {identifier!r}")

        # The regex already guaranteed 1-3 well-formed, dot-separated parts, so
        # the shared scanner yields exactly those parts (unquoted value + flag).
        parts = [
            quoted_name(value, quote=was_quoted)
            for value, was_quoted in split_identifier_parts(identifier)
        ]
        if len(parts) == 1:
            return cls(name=parts[0])
        if len(parts) == 2:
            return cls(name=parts[1], schema=parts[0])
        return cls(name=parts[2], schema=parts[1], database=parts[0])

    def _key(self) -> tuple:
        return tuple((str(p), bool(getattr(p, "quote", None))) for p in self.parts)

    def __str__(self) -> str:
        return self.identifier

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FQN):
            return NotImplemented
        return self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())

    def __repr__(self) -> str:
        return f"FQN(database={self._database!r}, schema={self._schema!r}, name={self._name!r})"
