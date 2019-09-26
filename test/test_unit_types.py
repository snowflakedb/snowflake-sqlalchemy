#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import snowflake.sqlalchemy


def test_type_synonyms():
    from snowflake.sqlalchemy.snowdialect import ischema_names
    for k, v in ischema_names.items():
        assert getattr(snowflake.sqlalchemy, k) is not None
