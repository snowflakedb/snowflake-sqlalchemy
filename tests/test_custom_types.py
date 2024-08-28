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

from snowflake.sqlalchemy import custom_types


def test_string_conversions():
    """Makes sure that all of the Snowflake SQLAlchemy types can be turned into Strings"""
    sf_custom_types = [
        "VARIANT",
        "OBJECT",
        "ARRAY",
        "TIMESTAMP_TZ",
        "TIMESTAMP_LTZ",
        "TIMESTAMP_NTZ",
        "GEOGRAPHY",
        "GEOMETRY",
    ]
    sf_types = [
        "TEXT",
        "CHARACTER",
        "DEC",
        "DOUBLE",
        "FIXED",
        "NUMBER",
        "BYTEINT",
        "STRING",
        "TINYINT",
        "VARBINARY",
    ] + sf_custom_types

    for type_ in sf_types:
        sample = getattr(custom_types, type_)()
        if type_ in sf_custom_types:
            assert type_ == str(sample)
