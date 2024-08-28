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
from sqlalchemy.sql.roles import FromClauseRole


class ExternalStage(ClauseElement, FromClauseRole):
    """External Stage descriptor"""

    __visit_name__ = "external_stage"
    _hide_froms = ()

    @staticmethod
    def prepare_namespace(namespace):
        return f"{namespace}." if not namespace.endswith(".") else namespace

    @staticmethod
    def prepare_path(path):
        return f"/{path}" if not path.startswith("/") else path

    def __init__(self, name, path=None, namespace=None, file_format=None):
        self.name = name
        self.path = self.prepare_path(path) if path else ""
        self.namespace = self.prepare_namespace(namespace) if namespace else ""
        self.file_format = file_format

    def __repr__(self):
        return f"@{self.namespace}{self.name}{self.path} ({self.file_format})"

    @classmethod
    def from_parent_stage(cls, parent_stage, path, file_format=None):
        """
        Extend an existing parent stage (with or without path) with an
        additional sub-path
        """
        return cls(
            parent_stage.name,
            f"{parent_stage.path}/{path}",
            parent_stage.namespace,
            file_format,
        )
