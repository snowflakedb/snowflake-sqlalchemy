#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2019 Snowflake Computing Inc. All right reserved.
#

import pytest
from sqlalchemy import Column, ForeignKey, Integer, Sequence, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship


def test_basic_orm(engine_testaccount, connection_type):
    """
    Tests declarative
    """
    if connection_type == "mock":
        pytest.skip()
    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return "<User(%r, %r)>" % (self.name, self.fullname)

    Base.metadata.create_all(engine_testaccount)
    try:
        ed_user = User(name='ed', fullname='Edward Jones')

        session = Session(bind=engine_testaccount)
        session.add(ed_user)

        our_user = session.query(User).filter_by(name='ed').first()
        assert our_user == ed_user
        session.commit()
    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_orm_one_to_many_relationship(engine_testaccount, connection_type):
    """
    Tests One to Many relationship
    """
    if connection_type == "mock":
        pytest.skip()
    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return "<User(%r, %r)>" % (self.name, self.fullname)

    class Address(Base):
        __tablename__ = 'address'

        id = Column(Integer, Sequence('address_id_seq'), primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey('user.id'))

        user = relationship("User", backref='addresses')

        def __repr__(self):
            return "<Address(%r)>" % self.email_address

    Base.metadata.create_all(engine_testaccount)

    try:
        jack = User(name='jack', fullname="Jack Bean")
        assert jack.addresses == [], 'one to many record is empty list'

        jack.addresses = [
            Address(email_address='jack@gmail.com'),
            Address(email_address='j25@yahoo.com'),
            Address(email_address='jack@hotmail.com'),
        ]

        session = Session(bind=engine_testaccount)
        session.add(jack)  # cascade each Address into the Session as well
        session.commit()

        got_jack = session.query(User).first()
        assert got_jack == jack

        bob = User(name='bob', fullname='Bob Dyran')

        session.add(bob)
        got_bob = session.query(User).filter_by(name='bob').first()
        assert got_bob == bob
        session.rollback()
        got_whoever = session.query(User).all()
        assert len(got_whoever) == 1, 'number of user'
        assert got_whoever[0] == jack

        session.delete(jack)
        got_addresses = session.query(Address).all()
        assert len(got_addresses) == 3, ("address records still remain in no "
                                         "cascade mode")

    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_delete_cascade(engine_testaccount, connection_type):
    """
    Test delete cascade
    """
    if connection_type == "mock":
        pytest.skip()
    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        addresses = relationship("Address", back_populates='user',
                                 cascade="all, delete, delete-orphan")

        def __repr__(self):
            return "<User(%r, %r)>" % (self.name, self.fullname)

    class Address(Base):
        __tablename__ = 'address'

        id = Column(Integer, Sequence('address_id_seq'), primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey('user.id'))

        user = relationship("User", back_populates="addresses")

        def __repr__(self):
            return "<Address(%r)>" % self.email_address

    Base.metadata.create_all(engine_testaccount)

    try:
        jack = User(name='jack', fullname="Jack Bean")
        assert jack.addresses == [], 'one to many record is empty list'

        jack.addresses = [
            Address(email_address='jack@gmail.com'),
            Address(email_address='j25@yahoo.com'),
            Address(email_address='jack@hotmail.com'),
        ]

        session = Session(bind=engine_testaccount)
        session.add(jack)  # cascade each Address into the Session as well
        session.commit()

        got_jack = session.query(User).first()
        assert got_jack == jack

        session.delete(jack)
        got_addresses = session.query(Address).all()
        assert len(got_addresses) == 0, ("no address record")
    finally:
        Base.metadata.drop_all(engine_testaccount)


@pytest.mark.skipif(True, reason="""
WIP
""")
def test_orm_query(engine_testaccount):
    """
    Tests ORM query
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = 'user'

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return "<User(%r, %r)>" % (self.name, self.fullname)

    Base.metadata.create_all(engine_testaccount)

    # TODO: insert rows

    session = Session(bind=engine_testaccount)

    # TODO: query.all()
    for name, fullname in session.query(User.name, User.fullname):
        print(name, fullname)

        # TODO: session.query.one() must return always one. NoResultFound and
        # MultipleResultsFound if not one result


def test_schema_including_db(engine_testaccount, db_parameters, connection_type):
    """
    Test schema parameter including database separated by a dot.
    """
    if connection_type == "mock":
        pytest.skip()
    Base = declarative_base()

    namespace = '{0}.{1}'.format(
        db_parameters['database'], db_parameters['schema'])

    class User(Base):
        __tablename__ = 'users'
        __table_args__ = {
            'schema': namespace
        }

        id = Column(Integer, Sequence('user_id_orm_seq', schema=namespace),
                    primary_key=True)
        name = Column(String)
        fullname = Column(String)

    Base.metadata.create_all(engine_testaccount)
    try:
        ed_user = User(name='ed', fullname='Edward Jones')

        session = Session(bind=engine_testaccount)
        session.add(ed_user)

        ret_user = session.query(User.id, User.name).first()
        assert ret_user[0] == 1
        assert ret_user[1] == 'ed'

        session.commit()
    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_schema_including_dot(engine_testaccount, db_parameters):
    """
    Tests pseudo schema name including dot.
    """
    Base = declarative_base()

    namespace = '{db}."{schema}.{schema}".{db}'.format(
        db=db_parameters['database'].lower(),
        schema=db_parameters['schema'].lower())

    class User(Base):
        __tablename__ = 'users'
        __table_args__ = {
            'schema': namespace
        }

        id = Column(Integer, Sequence('user_id_orm_seq', schema=namespace),
                    primary_key=True)
        name = Column(String)
        fullname = Column(String)

    session = Session(bind=engine_testaccount)
    query = session.query(User.id)
    assert str(query).startswith(
        'SELECT {db}."{schema}.{schema}".{db}.users.id'.format(
            db=db_parameters['database'].lower(),
            schema=db_parameters['schema'].lower()))
