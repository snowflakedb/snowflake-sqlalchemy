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


class CreateStage(DDLElement):
    """
    Encapsulates a CREATE STAGE statement, using a container (physical base for the
    stage) and the actual ExternalStage object.
    """

    __visit_name__ = "create_stage"

    def __init__(self, container, stage, replace_if_exists=False, *, temporary=False):
        super().__init__()
        self.container = container
        self.temporary = temporary
        self.stage = stage
        self.replace_if_exists = replace_if_exists
