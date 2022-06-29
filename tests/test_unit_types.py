#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import snowflake.sqlalchemy


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names

    for k, _ in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None
