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

from sqlalchemy.sql import compiler


class SnowflakeTypeCompiler(compiler.GenericTypeCompiler):
    def visit_BYTEINT(self, type_, **kw):
        return "BYTEINT"

    def visit_CHARACTER(self, type_, **kw):
        return "CHARACTER"

    def visit_DEC(self, type_, **kw):
        return "DEC"

    def visit_DOUBLE(self, type_, **kw):
        return "DOUBLE"

    def visit_FIXED(self, type_, **kw):
        return "FIXED"

    def visit_INT(self, type_, **kw):
        return "INT"

    def visit_NUMBER(self, type_, **kw):
        return "NUMBER"

    def visit_STRING(self, type_, **kw):
        return "STRING"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_VARIANT(self, type_, **kw):
        return "VARIANT"

    def visit_ARRAY(self, type_, **kw):
        return "ARRAY"

    def visit_OBJECT(self, type_, **kw):
        return "OBJECT"

    def visit_BLOB(self, type_, **kw):
        return "BINARY"

    def visit_datetime(self, type_, **kw):
        return "datetime"

    def visit_DATETIME(self, type_, **kw):
        return "DATETIME"

    def visit_TIMESTAMP_NTZ(self, type_, **kw):
        return "TIMESTAMP_NTZ"

    def visit_TIMESTAMP_TZ(self, type_, **kw):
        return "TIMESTAMP_TZ"

    def visit_TIMESTAMP_LTZ(self, type_, **kw):
        return "TIMESTAMP_LTZ"

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_GEOGRAPHY(self, type_, **kw):
        return "GEOGRAPHY"

    def visit_GEOMETRY(self, type_, **kw):
        return "GEOMETRY"
