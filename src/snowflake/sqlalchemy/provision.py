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

from sqlalchemy.testing.provision import set_default_schema_on_connection


# This is only for test purpose required by Requirement "default_schema_name_switch"
@set_default_schema_on_connection.for_db("snowflake")
def _snowflake_set_default_schema_on_connection(cfg, dbapi_connection, schema_name):
    cursor = dbapi_connection.cursor()
    cursor.execute(f"USE SCHEMA {dbapi_connection.database}.{schema_name};")
    cursor.close()
