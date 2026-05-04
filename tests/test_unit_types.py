#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import snowflake.sqlalchemy
from snowflake.sqlalchemy.compat import IS_VERSION_20
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from .util import ischema_names_baseline

_SA20_ONLY_TYPES = {"UUID"} if IS_VERSION_20 else set()


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names

    for k, _ in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None


def test_type_baseline():
    assert (
        set(SnowflakeDialect.ischema_names.keys())
        == set(ischema_names_baseline.keys()) | _SA20_ONLY_TYPES
    )
    for k, v in SnowflakeDialect.ischema_names.items():
        if k in _SA20_ONLY_TYPES:
            continue
        assert issubclass(v, ischema_names_baseline[k])
