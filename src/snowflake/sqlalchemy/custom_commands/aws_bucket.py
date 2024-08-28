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

from sqlalchemy import ClauseElement

from snowflake.sqlalchemy.compat import string_types


class AWSBucket(ClauseElement):
    """AWS S3 bucket descriptor"""

    __visit_name__ = "aws_bucket"

    def __init__(self, bucket, path=None):
        self.bucket = bucket
        self.path = path
        self.encryption_used = {}
        self.credentials_used = {}

    @classmethod
    def from_uri(cls, uri):
        if uri[0:5] != "s3://":
            raise ValueError(f"Invalid AWS bucket URI: {uri}")
        b = uri[5:].split("/", 1)
        if len(b) == 1:
            bucket, path = b[0], None
        else:
            bucket, path = b
        return cls(bucket, path)

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
        uri = "'s3://{}{}'".format(self.bucket, f"/{self.path}" if self.path else "")
        return "{}{}{}".format(
            uri,
            f" {credentials}" if self.credentials_used else "",
            f" {encryption}" if self.encryption_used else "",
        )

    def credentials(
        self, aws_role=None, aws_key_id=None, aws_secret_key=None, aws_token=None
    ):
        if aws_role is None and (aws_key_id is None and aws_secret_key is None):
            raise ValueError(
                "Either 'aws_role', or aws_key_id and aws_secret_key has to be supplied"
            )
        if aws_role:
            self.credentials_used = {"AWS_ROLE": aws_role}
        else:
            self.credentials_used = {
                "AWS_SECRET_KEY": aws_secret_key,
                "AWS_KEY_ID": aws_key_id,
            }
            if aws_token:
                self.credentials_used["AWS_TOKEN"] = aws_token
        return self

    def encryption_aws_cse(self, master_key):
        self.encryption_used = {"TYPE": "AWS_CSE", "MASTER_KEY": master_key}
        return self

    def encryption_aws_sse_s3(self):
        self.encryption_used = {"TYPE": "AWS_SSE_S3"}
        return self

    def encryption_aws_sse_kms(self, kms_key_id=None):
        self.encryption_used = {"TYPE": "AWS_SSE_KMS"}
        if kms_key_id:
            self.encryption_used["KMS_KEY_ID"] = kms_key_id
        return self
