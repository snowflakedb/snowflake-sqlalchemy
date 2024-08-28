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

import snowflake.sqlalchemy
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from .util import ischema_names_baseline


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names

    for k, _ in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None


def test_type_baseline():
    assert set(SnowflakeDialect.ischema_names.keys()) == set(
        ischema_names_baseline.keys()
    )
    for k, v in SnowflakeDialect.ischema_names.items():
        assert issubclass(v, ischema_names_baseline[k])
