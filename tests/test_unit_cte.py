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


def test_cte():
    from datetime import date

    from sqlalchemy import Column, Date, Integer, MetaData, Table, literal, select

    from snowflake.sqlalchemy import snowdialect

    metadata = MetaData()
    visitors = Table(
        "visitors",
        metadata,
        Column("product_id", Integer),
        Column("date1", Date),
        Column("count", Integer),
    )
    product_id = 1
    day = date.today()
    count = 5
    with_bar = select(literal(product_id), literal(day), literal(count)).cte("bar")
    sel = select(with_bar)
    ins = visitors.insert().from_select(
        [visitors.c.product_id, visitors.c.date1, visitors.c.count], sel
    )
    assert str(ins.compile(dialect=snowdialect.dialect())) == (
        "INSERT INTO visitors (product_id, date1, count) WITH bar AS \n"
        "(SELECT %(param_1)s AS anon_1, %(param_2)s AS anon_2, %(param_3)s AS anon_3)\n"
        " SELECT bar.anon_1, bar.anon_2, bar.anon_3 \n"
        "FROM bar"
    )
