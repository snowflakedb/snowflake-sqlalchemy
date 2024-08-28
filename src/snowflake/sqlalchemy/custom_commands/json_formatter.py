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

from sqlalchemy.util import NoneType

from ..compat import string_types
from .copy_formatter import CopyFormatter


class JSONFormatter(CopyFormatter):
    """Format specific functions"""

    file_format = "json"

    def compression(self, comp_type):
        """String (constant) that specifies to compresses the unloaded data files using the specified compression algorithm."""
        if isinstance(comp_type, string_types):
            comp_type = comp_type.lower()
        _available_options = [
            "auto",
            "gzip",
            "bz2",
            "brotli",
            "zstd",
            "deflate",
            "raw_deflate",
            None,
        ]
        if comp_type not in _available_options:
            raise TypeError(f"Compression type should be one of : {_available_options}")
        self.options["COMPRESSION"] = comp_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service.
        """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options["FILE_EXTENSION"] = ext
        return self
