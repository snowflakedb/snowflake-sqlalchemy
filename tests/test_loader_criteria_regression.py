#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""Regression test: with_loader_criteria must work when snowflake-sqlalchemy is imported.

This test verifies that the global CompileState plugin registration does not
break SQLAlchemy's with_loader_criteria for non-Snowflake dialects.
"""

import sqlalchemy as sa
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.dialects import sqlite
from sqlalchemy.orm import DeclarativeBase, with_loader_criteria


class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parents"
    id = Column(Integer, primary_key=True)
    tenant_id = Column(String)


class Child(Base):
    __tablename__ = "children"
    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey("parents.id"))
    tenant_id = Column(String)


class GrandChild(Base):
    __tablename__ = "grandchildren"
    id = Column(Integer, primary_key=True)
    child_id = Column(Integer, ForeignKey("children.id"))
    is_active = Column(sa.Boolean, server_default="true")


def test_with_loader_criteria_in_subquery_non_snowflake_dialect():
    """with_loader_criteria must inject filters into sealed subqueries on non-Snowflake dialects."""
    child_subq = (
        sa.select(Child.parent_id, sa.func.count().label("cnt"))
        .join(GrandChild, GrandChild.child_id == Child.id)
        .group_by(Child.parent_id)
        .subquery("child_counts")
    )
    stmt = (
        sa.select(Parent, child_subq.c.cnt)
        .outerjoin(child_subq, Parent.id == child_subq.c.parent_id)
    )
    stmt_with_criteria = stmt.options(
        with_loader_criteria(
            GrandChild, GrandChild.is_active == True, include_aliases=True
        )
    )
    compiled = str(
        stmt_with_criteria.compile(
            dialect=sqlite.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "is_active" in compiled, (
        "with_loader_criteria did not inject filter into sealed subquery"
    )


def test_with_loader_criteria_in_subquery_snowflake_dialect():
    """with_loader_criteria must inject filters into sealed subqueries on Snowflake dialect too."""
    from snowflake.sqlalchemy import snowdialect

    child_subq = (
        sa.select(Child.parent_id, sa.func.count().label("cnt"))
        .join(GrandChild, GrandChild.child_id == Child.id)
        .group_by(Child.parent_id)
        .subquery("child_counts")
    )
    stmt = (
        sa.select(Parent, child_subq.c.cnt)
        .outerjoin(child_subq, Parent.id == child_subq.c.parent_id)
    )
    stmt_with_criteria = stmt.options(
        with_loader_criteria(
            GrandChild, GrandChild.is_active == True, include_aliases=True
        )
    )
    compiled = str(
        stmt_with_criteria.compile(
            dialect=snowdialect.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    )
    assert "is_active" in compiled, (
        "with_loader_criteria did not inject filter into sealed subquery"
    )
