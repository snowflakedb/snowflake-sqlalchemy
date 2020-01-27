#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

from snowflake.connector.compat import TO_UNICODE


def test_cte():
    from snowflake.sqlalchemy import snowdialect
    from datetime import date
    from sqlalchemy import (MetaData, Table, Column, Integer,
                            Date, select, literal)
    metadata = MetaData()
    visitors = Table('visitors', metadata, Column('product_id', Integer), Column('date1', Date),
                     Column('count', Integer))
    product_id = 1
    day = date.today()
    count = 5
    with_bar = select([literal(product_id), literal(day), literal(count)]).cte('bar')
    sel = select([with_bar])
    ins = visitors.insert().from_select([visitors.c.product_id, visitors.c.date1, visitors.c.count], sel)
    assert TO_UNICODE(ins.compile(dialect=snowdialect.dialect())) == (
        "INSERT INTO visitors (product_id, date1, count) WITH bar AS \n"
        "(SELECT %(param_1)s AS anon_1, %(param_2)s AS anon_2, %(param_3)s AS anon_3)\n"
        " SELECT bar.anon_1, bar.anon_2, bar.anon_3 \n"
        "FROM bar"
    )
