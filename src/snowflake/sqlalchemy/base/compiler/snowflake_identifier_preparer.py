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


from sqlalchemy.sql import compiler
from sqlalchemy.sql.elements import quoted_name

from ..constants import RESERVED_WORDS


class SnowflakeIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = {x.lower() for x in RESERVED_WORDS}

    def __init__(self, dialect, **kw):
        quote = '"'

        super().__init__(dialect, initial_quote=quote, escape_quote=quote)

    def _quote_free_identifiers(self, *ids):
        """
        Unilaterally identifier-quote any number of strings.
        """
        return tuple(self.quote(i) for i in ids if i is not None)

    def quote_schema(self, schema, force=None):
        """
        Split schema by a dot and merge with required quotes
        """
        idents = self._split_schema_by_dot(schema)
        return ".".join(self._quote_free_identifiers(*idents))

    def format_label(self, label, name=None):
        n = name or label.name
        s = n.replace(self.escape_quote, "")

        if not isinstance(n, quoted_name) or n.quote is None:
            return self.quote(s)

        return self.quote_identifier(s) if n.quote else s

    def _split_schema_by_dot(self, schema):
        ret = []
        idx = 0
        pre_idx = 0
        in_quote = False
        while idx < len(schema):
            if not in_quote:
                if schema[idx] == "." and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    pre_idx = idx + 1
                elif schema[idx] == '"':
                    in_quote = True
                    pre_idx = idx + 1
            else:
                if schema[idx] == '"' and pre_idx < idx:
                    ret.append(schema[pre_idx:idx])
                    in_quote = False
                    pre_idx = idx + 1
            idx += 1
            if pre_idx < len(schema) and schema[pre_idx] == ".":
                pre_idx += 1
        if pre_idx < idx:
            ret.append(schema[pre_idx:idx])

        # convert the returning strings back to quoted_name types, and assign the original 'quote' attribute on it
        quoted_ret = [
            quoted_name(value, quote=getattr(schema, "quote", None)) for value in ret
        ]

        return quoted_ret
