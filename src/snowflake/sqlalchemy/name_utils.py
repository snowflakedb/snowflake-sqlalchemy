#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.sql.elements import quoted_name


class _NameUtils:
    def __init__(self, identifier_preparer: IdentifierPreparer) -> None:
        self.identifier_preparer = identifier_preparer

    def normalize_name(self, name):
        if name is None:
            return None
        if name == "":
            return quoted_name(name, quote=True)
        if name.upper() == name and not self.identifier_preparer._requires_quotes(
            name.lower()
        ):
            return name.lower()
        elif name.lower() == name:
            return quoted_name(name, quote=True)
        else:
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

    def quote_for_sql(self, name):
        """Denormalize then conditionally quote a name for use in raw SQL."""
        denormalized = self.denormalize_name(name)
        if denormalized is None:
            return None
        if denormalized == "":
            return '""'
        return self.identifier_preparer.quote(denormalized)
