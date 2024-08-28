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

from snowflake.sqlalchemy.compat import string_types


class AzureContainer(ClauseElement):
    """Microsoft Azure Container descriptor"""

    __visit_name__ = "azure_container"

    def __init__(self, account, container, path=None):
        self.account = account
        self.container = container
        self.path = path
        self.encryption_used = {}
        self.credentials_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:8] != "azure://":
            raise ValueError(f"Invalid Azure Container URI: {uri}")
        account, uri = uri[8:].split(".", 1)
        if uri[0:22] != "blob.core.windows.net/":
            raise ValueError(f"Invalid Azure Container URI: {uri}")
        b = uri[22:].split("/", 1)
        if len(b) == 1:
            container, path = b[0], None
        else:
            container, path = b
        return cls(account, container, path)

    def __repr__(self):
        credentials = "CREDENTIALS=({})".format(
            " ".join(f"{n}='{v}'" for n, v in self.credentials_used.items())
        )
        encryption = "ENCRYPTION=({})".format(
            " ".join(
                f"{n}='{v}'" if isinstance(v, string_types) else f"{n}={v}"
                for n, v in self.encryption_used.items()
            )
        )
        uri = "'azure://{}.blob.core.windows.net/{}{}'".format(
            self.account, self.container, f"/{self.path}" if self.path else ""
        )
        return "{}{}{}".format(
            uri,
            f" {credentials}" if self.credentials_used else "",
            f" {encryption}" if self.encryption_used else "",
        )

    def credentials(self, azure_sas_token):
        self.credentials_used = {"AZURE_SAS_TOKEN": azure_sas_token}
        return self

    def encryption_azure_cse(self, master_key):
        self.encryption_used = {"TYPE": "AZURE_CSE", "MASTER_KEY": master_key}
        return self
