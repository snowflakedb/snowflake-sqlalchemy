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

from typing import List

from sqlalchemy.sql.dml import UpdateBase

from snowflake.sqlalchemy.custom_commands.utils import translate_bool


class FilesOption:
    """
    Class to represent FILES option for the snowflake COPY INTO statement
    """

    def __init__(self, file_names: List[str]):
        self.file_names = file_names

    def __str__(self):
        the_files = ["'" + f.replace("'", "\\'") + "'" for f in self.file_names]
        return f"({','.join(the_files)})"


class CopyInto(UpdateBase):
    """Copy Into Command base class, for documentation see:
    https://docs.snowflake.net/manuals/sql-reference/sql/copy-into-location.html"""

    __visit_name__ = "copy_into"
    _bind = None

    def __init__(self, from_, into, formatter=None):
        self.from_ = from_
        self.into = into
        self.formatter = formatter
        self.copy_options = {}

    def __repr__(self):
        """
        repr for debugging / logging purposes only. For compilation logic, see
        the corresponding visitor in base.py
        """
        return f"COPY INTO {self.into} FROM {repr(self.from_)} {repr(self.formatter)} ({self.copy_options})"

    def bind(self):
        return None

    def force(self, force):
        if not isinstance(force, bool):
            raise TypeError("Parameter force should be a boolean value")
        self.copy_options.update({"FORCE": translate_bool(force)})
        return self

    def single(self, single_file):
        if not isinstance(single_file, bool):
            raise TypeError("Parameter single_file should  be a boolean value")
        self.copy_options.update({"SINGLE": translate_bool(single_file)})
        return self

    def maxfilesize(self, max_size):
        if not isinstance(max_size, int):
            raise TypeError("Parameter max_size should be an integer value")
        self.copy_options.update({"MAX_FILE_SIZE": max_size})
        return self

    def files(self, file_names):
        self.copy_options.update({"FILES": FilesOption(file_names)})
        return self

    def pattern(self, pattern):
        self.copy_options.update({"PATTERN": pattern})
        return self


CopyIntoStorage = CopyInto
