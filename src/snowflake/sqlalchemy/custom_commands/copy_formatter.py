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


from sqlalchemy.sql.elements import ClauseElement


class CopyFormatter(ClauseElement):
    """
    Base class for Formatter specifications inside a COPY INTO statement. May also
    be used to create a named format.
    """

    __visit_name__ = "copy_formatter"

    def __init__(self, format_name=None):
        self.options = dict()
        if format_name:
            self.options["format_name"] = format_name

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        return f"FILE_FORMAT=({self.options})"

    @staticmethod
    def value_repr(name, value):
        """
        Make a SQL-suitable representation of "value". This is called from
        the corresponding visitor function (base.py/visit_copy_formatter())
        - in case of a format name: return it without quotes
        - in case of a string: enclose in quotes: "value"
        - in case of a tuple of length 1: enclose the only element in brackets: (value)
            Standard stringification of Python would append a trailing comma: (value,)
            which is not correct in SQL
        - otherwise: just convert to str as is: value
        """
        if name == "format_name":
            return value
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, tuple) and len(value) == 1:
            return f"('{value[0]}')"
        else:
            return str(value)
