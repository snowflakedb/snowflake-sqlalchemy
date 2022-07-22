#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import enum

import pytest
from sqlalchemy import Column, Enum, ForeignKey, Integer, Sequence, String, text
from sqlalchemy.orm import Session, declarative_base, relationship


def test_basic_orm(engine_testaccount, run_v20_sqlalchemy):
    """
    Tests declarative
    """
    Base = declarative_base()

    class UserStatus(enum.Enum):
        ACTIVE = ("active",)
        INACTIVE = "inactive"

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        fullname = Column(String)
        status = Column(Enum(UserStatus), default=UserStatus.ACTIVE)

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    Base.metadata.create_all(engine_testaccount)
    try:
        ed_user = User(name="ed", fullname="Edward Jones")

        session = Session(bind=engine_testaccount)
        session.future = run_v20_sqlalchemy
        session.add(ed_user)

        our_user = session.query(User).filter_by(name="ed").first()
        assert our_user == ed_user
        session.commit()
    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_orm_one_to_many_relationship(engine_testaccount, run_v20_sqlalchemy):
    """
    Tests One to Many relationship
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, Sequence("address_id_seq"), primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey("user.id"))

        user = relationship("User", backref="addresses")

        def __repr__(self):
            return "<Address(%r)>" % self.email_address

    Base.metadata.create_all(engine_testaccount)

    try:
        jack = User(name="jack", fullname="Jack Bean")
        assert jack.addresses == [], "one to many record is empty list"

        jack.addresses = [
            Address(email_address="jack@gmail.com"),
            Address(email_address="j25@yahoo.com"),
            Address(email_address="jack@hotmail.com"),
        ]

        session = Session(bind=engine_testaccount)
        session.future = run_v20_sqlalchemy
        session.add(jack)  # cascade each Address into the Session as well
        session.commit()

        got_jack = session.query(User).first()
        assert got_jack == jack

        bob = User(name="bob", fullname="Bob Dyran")

        session.add(bob)
        got_bob = session.query(User).filter_by(name="bob").first()
        assert got_bob == bob
        session.rollback()
        got_whoever = session.query(User).all()
        assert len(got_whoever) == 1, "number of user"
        assert got_whoever[0] == jack

        session.delete(jack)
        got_addresses = session.query(Address).all()
        assert len(got_addresses) == 3, (
            "address records still remain in no " "cascade mode"
        )

    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_delete_cascade(engine_testaccount, run_v20_sqlalchemy):
    """
    Test delete cascade
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        name = Column(String)
        fullname = Column(String)

        addresses = relationship(
            "Address", back_populates="user", cascade="all, delete, delete-orphan"
        )

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    class Address(Base):
        __tablename__ = "address"

        id = Column(Integer, Sequence("address_id_seq"), primary_key=True)
        email_address = Column(String, nullable=False)
        user_id = Column(Integer, ForeignKey("user.id"))

        user = relationship("User", back_populates="addresses")

        def __repr__(self):
            return "<Address(%r)>" % self.email_address

    Base.metadata.create_all(engine_testaccount)

    try:
        jack = User(name="jack", fullname="Jack Bean")
        assert jack.addresses == [], "one to many record is empty list"

        jack.addresses = [
            Address(email_address="jack@gmail.com"),
            Address(email_address="j25@yahoo.com"),
            Address(email_address="jack@hotmail.com"),
        ]

        session = Session(bind=engine_testaccount)
        session.future = run_v20_sqlalchemy
        session.add(jack)  # cascade each Address into the Session as well
        session.commit()

        got_jack = session.query(User).first()
        assert got_jack == jack

        session.delete(jack)
        got_addresses = session.query(Address).all()
        assert len(got_addresses) == 0, "no address record"
    finally:
        Base.metadata.drop_all(engine_testaccount)


@pytest.mark.skipif(
    True,
    reason="""
WIP
""",
)
def test_orm_query(engine_testaccount, run_v20_sqlalchemy):
    """
    Tests ORM query
    """
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"

        id = Column(Integer, primary_key=True)
        name = Column(String)
        fullname = Column(String)

        def __repr__(self):
            return f"<User({self.name!r}, {self.fullname!r})>"

    Base.metadata.create_all(engine_testaccount)

    # TODO: insert rows

    session = Session(bind=engine_testaccount)
    session.future = run_v20_sqlalchemy

    # TODO: query.all()
    for name, fullname in session.query(User.name, User.fullname):
        print(name, fullname)

        # TODO: session.query.one() must return always one. NoResultFound and
        # MultipleResultsFound if not one result


def test_schema_including_db(engine_testaccount, db_parameters, run_v20_sqlalchemy):
    """
    Test schema parameter including database separated by a dot.
    """
    Base = declarative_base()

    namespace = "{}.{}".format(db_parameters["database"], db_parameters["schema"])

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": namespace}

        id = Column(
            Integer, Sequence("user_id_orm_seq", schema=namespace), primary_key=True
        )
        name = Column(String)
        fullname = Column(String)

    Base.metadata.create_all(engine_testaccount)
    try:
        ed_user = User(name="ed", fullname="Edward Jones")

        session = Session(bind=engine_testaccount)
        session.future = run_v20_sqlalchemy
        session.add(ed_user)

        ret_user = session.query(User.id, User.name).first()
        assert ret_user[0] == 1
        assert ret_user[1] == "ed"

        session.commit()
    finally:
        Base.metadata.drop_all(engine_testaccount)


def test_schema_including_dot(engine_testaccount, db_parameters, run_v20_sqlalchemy):
    """
    Tests pseudo schema name including dot.
    """
    Base = declarative_base()

    namespace = '{db}."{schema}.{schema}".{db}'.format(
        db=db_parameters["database"].lower(), schema=db_parameters["schema"].lower()
    )

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": namespace}

        id = Column(
            Integer, Sequence("user_id_orm_seq", schema=namespace), primary_key=True
        )
        name = Column(String)
        fullname = Column(String)

    session = Session(bind=engine_testaccount)
    session.future = run_v20_sqlalchemy
    query = session.query(User.id)
    assert str(query).startswith(
        'SELECT {db}."{schema}.{schema}".{db}.users.id'.format(
            db=db_parameters["database"].lower(), schema=db_parameters["schema"].lower()
        )
    )


def test_schema_translate_map(
    engine_testaccount, db_parameters, sql_compiler, run_v20_sqlalchemy
):
    """
    Test schema translate map execution option works replaces schema correctly
    """
    Base = declarative_base()

    namespace = f"{db_parameters['database']}.{db_parameters['schema']}"
    schema_map = "A"

    class User(Base):
        __tablename__ = "users"
        __table_args__ = {"schema": schema_map}

        id = Column(
            Integer, Sequence("user_id_orm_seq", schema=namespace), primary_key=True
        )
        name = Column(String)
        fullname = Column(String)

    with engine_testaccount.connect().execution_options(
        schema_translate_map={schema_map: db_parameters["schema"]}
    ) as con:
        session = Session(bind=con)
        session.future = run_v20_sqlalchemy
        with con.begin():
            Base.metadata.create_all(con)
        try:
            query = session.query(User)

            # insert some data in a way that makes sure that we're working in the right testing schema
            with con.begin():
                con.execute(
                    text(
                        f"insert into {db_parameters['schema']}.{User.__tablename__} values (0, 'testuser', 'test_user')"
                    )
                )

            # assert the precompiled query contains the schema_map and not the actual schema
            assert str(query).startswith(f'SELECT "{schema_map}".{User.__tablename__}')

            # run query and see that schema translation was done corectly
            results = query.all()
            assert len(results) == 1
            user = results.pop()
            assert user.id == 0
            assert user.name == "testuser"
            assert user.fullname == "test_user"
        finally:
            Base.metadata.drop_all(con)
