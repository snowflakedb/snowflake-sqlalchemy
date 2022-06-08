#!/usr/bin/env python
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import snowflake.sqlalchemy


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names

    for k, _ in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None
