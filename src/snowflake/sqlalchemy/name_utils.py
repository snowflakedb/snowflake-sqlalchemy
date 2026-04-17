#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

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
