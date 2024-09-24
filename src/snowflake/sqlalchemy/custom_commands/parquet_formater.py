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

from snowflake.sqlalchemy.custom_commands.copy_formatter import CopyFormatter
from snowflake.sqlalchemy.custom_commands.utils import translate_bool


class PARQUETFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = "parquet"

    def snappy_compression(self, comp):
        """Enable, or disable snappy compression"""
        if not isinstance(comp, bool):
            raise TypeError("Comp should be a Boolean value")
        self.options["SNAPPY_COMPRESSION"] = translate_bool(comp)
        return self

    def compression(self, comp):
        """
        Set compression type
        """
        if not isinstance(comp, str):
            raise TypeError("Comp should be a str value")
        self.options["COMPRESSION"] = comp
        return self

    def binary_as_text(self, value):
        """Enable, or disable binary as text"""
        if not isinstance(value, bool):
            raise TypeError("binary_as_text should be a Boolean value")
        self.options["BINARY_AS_TEXT"] = translate_bool(value)
        return self
