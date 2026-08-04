#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Lightweight fully-qualified-name (FQN) parsing for Snowflake identifiers.

The identifier regexes and the parsing approach are adapted from Snowflake CLI
(``snowflake.cli.api.identifiers`` / ``snowflake.cli.api.project.util``), which
is licensed under the Apache License 2.0 -- the same license as this project.
Only the minimal subset needed to split and render ``[[db.]schema.]name`` is
kept here; the dialect decides per-part quoting at render time.
"""

from __future__ import annotations

import re

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


def is_valid_quoted_identifier(identifier: str) -> bool:
    """Return True when ``identifier`` is a valid Snowflake quoted identifier."""
    return re.fullmatch(QUOTED_IDENTIFIER_REGEX, identifier) is not None


class FQN:
    """A Snowflake object name, optionally qualified by database and schema.

    Parse with :meth:`from_string`. Each part is kept as its raw token (a
    quoted token such as ``"Foo"`` is preserved verbatim) so the caller can
    apply the dialect's own quoting rules per part when rendering.
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

    @property
    def identifier(self) -> str:
        return ".".join(self.parts)

    @classmethod
    def from_string(cls, identifier: str) -> FQN:
        """Parse ``[[database.]schema.]name`` into an :class:`FQN`.

        Raises ``ValueError`` when ``identifier`` is not a well-formed
        (optionally qualified) Snowflake identifier.
        """
        match = re.fullmatch(_FQN_REGEX, identifier)
        if match is None:
            raise ValueError(f"Invalid Snowflake identifier: {identifier!r}")

        name = match.group("name")
        first = match.group("first")
        second = match.group("second")
        if second is not None:
            return cls(name=name, schema=second, database=first)
        return cls(name=name, schema=first, database=None)

    def __str__(self) -> str:
        return self.identifier

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FQN):
            return NotImplemented
        return self.parts == other.parts

    def __hash__(self) -> int:
        return hash(tuple(self.parts))

    def __repr__(self) -> str:
        return f"FQN(database={self._database!r}, schema={self._schema!r}, name={self._name!r})"
