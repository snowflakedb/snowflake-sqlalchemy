#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.sql.elements import quoted_name


class _NameUtils:

    def __init__(self, identifier_preparer: IdentifierPreparer) -> None:
        self.identifier_preparer = identifier_preparer

    def normalize_name(self, name):
        if name in (None, ""):
            return name

        if isinstance(name, quoted_name) and name.quote:
            return name

        coerced_name = str(name)
        has_lowercase = any(char.islower() for char in coerced_name)

        requires_quotes = has_lowercase
        if self.identifier_preparer is not None:
            requires_quotes = (
                requires_quotes
                or self.identifier_preparer._requires_quotes(coerced_name.lower())
            )

        if requires_quotes:
            return quoted_name(coerced_name, quote=True)

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
