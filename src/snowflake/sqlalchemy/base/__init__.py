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

from sqlalchemy.schema import Table
from sqlalchemy.sql import functions

from ..functions import flatten
from .compiler.snowflake_compiler import SnowflakeCompiler
from .compiler.snowflake_ddl_compiler import SnowflakeDDLCompiler
from .compiler.snowflake_identifier_preparer import SnowflakeIdentifierPreparer
from .compiler.snowflake_type_compiler import SnowflakeTypeCompiler
from .constants import RESERVED_WORDS
from .snowflake_execution_context import SnowflakeExecutionContext
from .snowflake_orm_select_compile_state import SnowflakeORMSelectCompileState
from .snowflake_select_state import SnowflakeSelectState

# The __all__ list is used to maintain backward compatibility with previous versions of the package.
# After splitting a large file into a module, __all__ explicitly defines the public API, ensuring
# that existing imports from the original file still work as expected. This approach prevents
# breaking changes by controlling which functions, classes, and variables are exposed when
# the module is imported.
__all__ = [
    "SnowflakeDDLCompiler",
    "SnowflakeCompiler",
    "SnowflakeIdentifierPreparer",
    "SnowflakeTypeCompiler",
    "SnowflakeExecutionContext",
    "SnowflakeORMSelectCompileState",
    "SnowflakeSelectState",
    "RESERVED_WORDS",
]


construct_arguments = [(Table, {"clusterby": None})]

functions.register_function("flatten", flatten)
