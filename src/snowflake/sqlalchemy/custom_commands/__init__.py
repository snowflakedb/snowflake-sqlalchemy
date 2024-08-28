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

from .aws_bucket import AWSBucket
from .azure_container import AzureContainer
from .copy_formatter import CopyFormatter
from .copy_into import CopyInto, CopyIntoStorage, FilesOption
from .create_file_format import CreateFileFormat
from .create_stage import CreateStage
from .csv_formatter import CSVFormatter
from .external_stage import ExternalStage
from .json_formatter import JSONFormatter
from .merge_into import MergeInto
from .parquet_formater import PARQUETFormatter
from .utils import translate_bool

# The __all__ list is used to maintain backward compatibility with previous versions of the package.
# After splitting a large file into a module, __all__ explicitly defines the public API, ensuring
# that existing imports from the original file still work as expected. This approach prevents
# breaking changes by controlling which functions, classes, and variables are exposed when
# the module is imported.
__all__ = [
    "AWSBucket",
    "AzureContainer",
    "CopyFormatter",
    "FilesOption",
    "CopyInto",
    "CopyIntoStorage",
    "CreateFileFormat",
    "CreateStage",
    "CSVFormatter",
    "ExternalStage",
    "JSONFormatter",
    "MergeInto",
    "PARQUETFormatter",
    "translate_bool",
]
