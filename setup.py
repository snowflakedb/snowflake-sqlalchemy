#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import os

from setuptools import setup

SQLALCHEMY_SRC_DIR = os.path.join("src", "snowflake", "sqlalchemy")
VERSION = (1, 1, 1, None)  # Default
with open(os.path.join(SQLALCHEMY_SRC_DIR, "version.py"), encoding="utf-8") as f:
    exec(f.read())
    version = ".".join([str(v) for v in VERSION if v is not None])

setup(
    version=version,
)
