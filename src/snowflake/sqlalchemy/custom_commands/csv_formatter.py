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

from collections.abc import Sequence

from sqlalchemy.util import NoneType

from snowflake.sqlalchemy.compat import string_types
from snowflake.sqlalchemy.custom_commands.copy_formatter import CopyFormatter


class CSVFormatter(CopyFormatter):
    file_format = "csv"

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

    def _check_delimiter(self, delimiter, delimiter_txt):
        """
        Check if a delimiter is either a string of length 1 or an integer. In case of
        a string delimiter, take into account that the actual string may be longer,
        but still evaluate to a single character (like "\\n" or r"\n"
        """
        if isinstance(delimiter, NoneType):
            return
        if isinstance(delimiter, string_types):
            delimiter_processed = delimiter.encode().decode("unicode_escape")
            if len(delimiter_processed) == 1:
                return
        if isinstance(delimiter, int):
            return
        raise TypeError(
            f"{delimiter_txt} should be a single character, that is either a string, or a number"
        )

    def record_delimiter(self, deli_type):
        """Character that separates records in an unloaded file."""
        self._check_delimiter(deli_type, "Record delimiter")
        if isinstance(deli_type, int):
            self.options["RECORD_DELIMITER"] = hex(deli_type)
        else:
            self.options["RECORD_DELIMITER"] = deli_type
        return self

    def field_delimiter(self, deli_type):
        """Character that separates fields in an unloaded file."""
        self._check_delimiter(deli_type, "Field delimiter")
        if isinstance(deli_type, int):
            self.options["FIELD_DELIMITER"] = hex(deli_type)
        else:
            self.options["FIELD_DELIMITER"] = deli_type
        return self

    def file_extension(self, ext):
        """String that specifies the extension for files unloaded to a stage. Accepts any extension. The user is
        responsible for specifying a valid file extension that can be read by the desired software or service.
        """
        if not isinstance(ext, (NoneType, string_types)):
            raise TypeError("File extension should be a string")
        self.options["FILE_EXTENSION"] = ext
        return self

    def date_format(self, dt_frmt):
        """String that defines the format of date values in the unloaded data files."""
        if not isinstance(dt_frmt, string_types):
            raise TypeError("Date format should be a string")
        self.options["DATE_FORMAT"] = dt_frmt
        return self

    def time_format(self, tm_frmt):
        """String that defines the format of time values in the unloaded data files."""
        if not isinstance(tm_frmt, string_types):
            raise TypeError("Time format should be a string")
        self.options["TIME_FORMAT"] = tm_frmt
        return self

    def timestamp_format(self, tmstmp_frmt):
        """String that defines the format of timestamp values in the unloaded data files."""
        if not isinstance(tmstmp_frmt, string_types):
            raise TypeError("Timestamp format should be a string")
        self.options["TIMESTAMP_FORMAT"] = tmstmp_frmt
        return self

    def binary_format(self, bin_fmt):
        """Character used as the escape character for any field values. The option can be used when unloading data
        from binary columns in a table."""
        if isinstance(bin_fmt, string_types):
            bin_fmt = bin_fmt.lower()
        _available_options = ["hex", "base64", "utf8"]
        if bin_fmt not in _available_options:
            raise TypeError(f"Binary format should be one of : {_available_options}")
        self.options["BINARY_FORMAT"] = bin_fmt
        return self

    def escape(self, esc):
        """Character used as the escape character for any field values."""
        self._check_delimiter(esc, "Escape")
        if isinstance(esc, int):
            self.options["ESCAPE"] = hex(esc)
        else:
            self.options["ESCAPE"] = esc
        return self

    def escape_unenclosed_field(self, esc):
        """Single character string used as the escape character for unenclosed field values only."""
        self._check_delimiter(esc, "Escape unenclosed field")
        if isinstance(esc, int):
            self.options["ESCAPE_UNENCLOSED_FIELD"] = hex(esc)
        else:
            self.options["ESCAPE_UNENCLOSED_FIELD"] = esc
        return self

    def field_optionally_enclosed_by(self, enc):
        """Character used to enclose strings. Either None, ', or \"."""
        _available_options = [None, "'", '"']
        if enc not in _available_options:
            raise TypeError(f"Enclosing string should be one of : {_available_options}")
        self.options["FIELD_OPTIONALLY_ENCLOSED_BY"] = enc
        return self

    def null_if(self, null_value):
        """Copying into a table these strings will be replaced by a NULL, while copying out of Snowflake will replace
        NULL values with the first string"""
        if not isinstance(null_value, Sequence):
            raise TypeError("Parameter null_value should be an iterable")
        self.options["NULL_IF"] = tuple(null_value)
        return self

    def skip_header(self, skip_header):
        """
        Number of header rows to be skipped at the beginning of the file
        """
        if not isinstance(skip_header, int):
            raise TypeError("skip_header  should be an int")
        self.options["SKIP_HEADER"] = skip_header
        return self

    def trim_space(self, trim_space):
        """
        Remove leading or trailing white spaces
        """
        if not isinstance(trim_space, bool):
            raise TypeError("trim_space should be a bool")
        self.options["TRIM_SPACE"] = trim_space
        return self

    def error_on_column_count_mismatch(self, error_on_col_count_mismatch):
        """
        Generate a parsing error if the number of delimited columns (i.e. fields) in
        an input data file does not match the number of columns in the corresponding table.
        """
        if not isinstance(error_on_col_count_mismatch, bool):
            raise TypeError("skip_header  should be a bool")
        self.options["ERROR_ON_COLUMN_COUNT_MISMATCH"] = error_on_col_count_mismatch
        return self
