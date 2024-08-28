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

from sqlalchemy import DDLElement


class CreateFileFormat(DDLElement):
    """
    Encapsulates a CREATE FILE FORMAT statement; using a format description (as in
    a COPY INTO statement) and a format name.
    """

    __visit_name__ = "create_file_format"

    def __init__(self, format_name, formatter, replace_if_exists=False):
        super().__init__()
        self.format_name = format_name
        self.formatter = formatter
        self.replace_if_exists = replace_if_exists
