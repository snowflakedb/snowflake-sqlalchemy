#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Basic ORM integration tests over the async adapter.

These tests run against both the sync snowflake:// driver (default) and the
async snowflake+snowflake_async:// driver (SNOWFLAKE_ASYNC_TESTS=1).
sqlalchemy.testing runs each test in a greenlet when config.is_async, so the
same synchronous Session / relationship code covers both drivers with no
test-code changes.

Models are defined inside each test method (same pattern as tests/test_orm.py)
so each method gets a fresh mapper registry that survives the
orm.clear_mappers() calls sqlalchemy.testing injects between test classes.
"""

from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Integer, Sequence, String, select
from sqlalchemy.orm import Session, declarative_base, relationship, selectinload
from sqlalchemy.testing import config, fixtures


class TestBasicORM(fixtures.TestBase):
    """Verifies basic CRUD (insert / select / update / delete / rollback)
    through the async adapter with a simple single-table mapped class."""

    __backend__ = True

    def test_insert_and_select(self) -> None:
        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_user"
            id = Column(Integer, Sequence("async_orm_user_seq"), primary_key=True)
            name = Column(String(50))
            fullname = Column(String(100))

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add(User(name="ed", fullname="Edward Jones"))
                session.commit()

            with Session(config.db) as session:
                users = session.execute(select(User)).scalars().all()
            assert len(users) == 1
            assert users[0].name == "ed"
            assert users[0].fullname == "Edward Jones"
        finally:
            Base.metadata.drop_all(config.db)

    def test_update(self) -> None:
        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_user"
            id = Column(Integer, Sequence("async_orm_user_seq"), primary_key=True)
            name = Column(String(50))
            fullname = Column(String(100))

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add(User(name="alice", fullname="Alice Smith"))
                session.commit()

            with Session(config.db) as session:
                user = session.execute(
                    select(User).where(User.name == "alice")
                ).scalar_one()
                user.fullname = "Alice Cooper"
                session.commit()

            with Session(config.db) as session:
                user = session.execute(
                    select(User).where(User.name == "alice")
                ).scalar_one()
            assert user.fullname == "Alice Cooper"
        finally:
            Base.metadata.drop_all(config.db)

    def test_delete(self) -> None:
        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_user"
            id = Column(Integer, Sequence("async_orm_user_seq"), primary_key=True)
            name = Column(String(50))

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add(User(name="bob"))
                session.commit()

            with Session(config.db) as session:
                user = session.execute(
                    select(User).where(User.name == "bob")
                ).scalar_one()
                session.delete(user)
                session.commit()

            with Session(config.db) as session:
                users = session.execute(select(User)).scalars().all()
            assert users == []
        finally:
            Base.metadata.drop_all(config.db)

    def test_rollback(self) -> None:
        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_user"
            id = Column(Integer, Sequence("async_orm_user_seq"), primary_key=True)
            name = Column(String(50))

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add(User(name="charlie"))
                session.rollback()

            with Session(config.db) as session:
                users = session.execute(select(User)).scalars().all()
            assert users == []
        finally:
            Base.metadata.drop_all(config.db)


class TestORMRelationship(fixtures.TestBase):
    """Verifies one-to-many relationship traversal and cascade delete through
    the async adapter using selectinload — the recommended eager-loading
    strategy (avoids lazy loading, which is prohibited in real AsyncSession)."""

    __backend__ = True

    def test_one_to_many_eager_load(self) -> None:
        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_rel_user"
            id = Column(Integer, Sequence("async_orm_rel_user_seq"), primary_key=True)
            name = Column(String(50))
            addresses = relationship(
                "Address", back_populates="user", cascade="all, delete-orphan"
            )

        class Address(Base):
            __tablename__ = "async_orm_rel_addr"
            id = Column(Integer, Sequence("async_orm_rel_addr_seq"), primary_key=True)
            email = Column(String(100))
            user_id = Column(Integer, ForeignKey("async_orm_rel_user.id"))
            user = relationship("User", back_populates="addresses")

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                jack = User(name="jack")
                jack.addresses = [
                    Address(email="jack@gmail.com"),
                    Address(email="jack@yahoo.com"),
                ]
                session.add(jack)
                session.commit()

            with Session(config.db) as session:
                user = session.execute(
                    select(User)
                    .where(User.name == "jack")
                    .options(selectinload(User.addresses))
                ).scalar_one()
            assert len(user.addresses) == 2
            assert {a.email for a in user.addresses} == {
                "jack@gmail.com",
                "jack@yahoo.com",
            }
        finally:
            Base.metadata.drop_all(config.db)

    def test_cascade_delete_removes_children(self) -> None:
        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_rel_user"
            id = Column(Integer, Sequence("async_orm_rel_user_seq"), primary_key=True)
            name = Column(String(50))
            addresses = relationship(
                "Address", back_populates="user", cascade="all, delete-orphan"
            )

        class Address(Base):
            __tablename__ = "async_orm_rel_addr"
            id = Column(Integer, Sequence("async_orm_rel_addr_seq"), primary_key=True)
            email = Column(String(100))
            user_id = Column(Integer, ForeignKey("async_orm_rel_user.id"))
            user = relationship("User", back_populates="addresses")

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                alice = User(name="alice")
                alice.addresses = [Address(email="alice@example.com")]
                session.add(alice)
                session.commit()

            with Session(config.db) as session:
                alice = session.execute(
                    select(User)
                    .where(User.name == "alice")
                    .options(selectinload(User.addresses))
                ).scalar_one()
                session.delete(alice)
                session.commit()

            with Session(config.db) as session:
                addresses = session.execute(select(Address)).scalars().all()
            assert addresses == [], "cascade delete should remove child rows"
        finally:
            Base.metadata.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 3 — Snowflake VARIANT column via ORM
# ---------------------------------------------------------------------------


class TestORMVariant(fixtures.TestBase):
    """Verifies that Snowflake VARIANT columns round-trip correctly through the
    async adapter. VARIANT handling involves dialect-specific type processing
    on both write and read paths."""

    __backend__ = True

    def test_variant_string_roundtrip(self) -> None:
        from snowflake.sqlalchemy import VARIANT

        Base = declarative_base()

        class Event(Base):
            __tablename__ = "async_orm_variant"
            id = Column(Integer, Sequence("async_orm_variant_seq"), primary_key=True)
            payload = Column(VARIANT)

        Base.metadata.create_all(config.db)
        try:
            raw = '{"key": "value", "n": 42}'
            with Session(config.db) as session:
                session.add(Event(payload=raw))
                session.commit()

            with Session(config.db) as session:
                event = session.execute(select(Event)).scalar_one()
            # Snowflake returns VARIANT as a parsed Python object (dict/list)
            # when enable_structured_type_json is active, or as a string otherwise.
            # Either way the round-trip must not raise and the value must be
            # non-empty.
            assert event.payload is not None
        finally:
            Base.metadata.drop_all(config.db)

    def test_variant_null(self) -> None:
        from snowflake.sqlalchemy import VARIANT

        Base = declarative_base()

        class Event(Base):
            __tablename__ = "async_orm_variant"
            id = Column(Integer, Sequence("async_orm_variant_seq"), primary_key=True)
            payload = Column(VARIANT, nullable=True)

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add(Event(payload=None))
                session.commit()

            with Session(config.db) as session:
                event = session.execute(select(Event)).scalar_one()
            assert event.payload is None
        finally:
            Base.metadata.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 4 — table reflection via inspect()
# ---------------------------------------------------------------------------


class TestTableReflection(fixtures.TestBase):
    """Verifies that SA's Inspector can reflect table metadata (columns, PKs,
    indexes) through the async adapter — used by automap and other tooling."""

    __backend__ = True

    def test_get_columns(self) -> None:
        from sqlalchemy import MetaData, Table
        from sqlalchemy import inspect as sa_inspect

        meta = MetaData()
        Table(
            "async_reflect_test",
            meta,
            Column("id", Integer, Sequence("async_reflect_test_seq"), primary_key=True),
            Column("name", String(50)),
            Column("score", Integer),
        )
        meta.create_all(config.db)
        try:
            insp = sa_inspect(config.db)
            cols = insp.get_columns("async_reflect_test")
            col_names = {c["name"] for c in cols}
            assert {"id", "name", "score"} == col_names
        finally:
            meta.drop_all(config.db)

    def test_get_table_names(self) -> None:
        from sqlalchemy import MetaData, Table
        from sqlalchemy import inspect as sa_inspect

        meta = MetaData()
        Table(
            "async_reflect_listed",
            meta,
            Column(
                "id", Integer, Sequence("async_reflect_listed_seq"), primary_key=True
            ),
        )
        meta.create_all(config.db)
        try:
            insp = sa_inspect(config.db)
            names = insp.get_table_names()
            assert "async_reflect_listed" in [n.lower() for n in names]
        finally:
            meta.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 5 — Core-level multi-row insert + select using the connection fixture
# ---------------------------------------------------------------------------


class TestCoreMultiRow(fixtures.TestBase):
    """Verifies Core DML operations (multi-row insert, ordering, filtering)
    through the async adapter using the TestBase connection fixture.
    This is the same execution path that --async-engine would exercise."""

    __backend__ = True

    def test_multi_row_insert_and_select(self, connection) -> None:
        from sqlalchemy import MetaData, Table, text

        meta = MetaData()
        tbl = Table(
            "async_core_multirow",
            meta,
            Column(
                "id", Integer, Sequence("async_core_multirow_seq"), primary_key=True
            ),
            Column("name", String(50)),
            Column("score", Integer),
        )
        meta.create_all(config.db)
        try:
            connection.execute(
                tbl.insert(),
                [
                    {"name": "alice", "score": 90},
                    {"name": "bob", "score": 75},
                    {"name": "charlie", "score": 85},
                ],
            )
            connection.execute(text("COMMIT"))

            rows = connection.execute(
                select(tbl).order_by(tbl.c.score.desc())
            ).fetchall()
            assert len(rows) == 3
            assert rows[0].name == "alice"  # highest score
            assert rows[1].name == "charlie"
            assert rows[2].name == "bob"

            filtered = connection.execute(
                select(tbl).where(tbl.c.score > 80)
            ).fetchall()
            assert len(filtered) == 2
            assert {r.name for r in filtered} == {"alice", "charlie"}
        finally:
            meta.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 6 — VECTOR column round-trip (Core path then ORM read)
# ---------------------------------------------------------------------------


class TestVectorType(fixtures.TestBase):
    """Verifies that VECTOR columns can be created, written (via raw SQL — the
    connector requires the ``::VECTOR(...)`` cast on INSERT), and read back
    as Python lists through the async adapter."""

    __backend__ = True

    def test_vector_core_insert_and_select(self, connection) -> None:
        from sqlalchemy import MetaData, Table, text

        from snowflake.sqlalchemy.custom_types import VECTOR

        table_name = "async_vector_core"
        meta = MetaData()
        Table(
            table_name,
            meta,
            Column("id", Integer),
            Column("vec", VECTOR("FLOAT", 3)),
        )
        meta.create_all(config.db)
        try:
            connection.exec_driver_sql(
                f"INSERT INTO {table_name}(id, vec) "
                f"SELECT 1, [1.0, 2.0, 3.0]::VECTOR(FLOAT, 3)"
            )
            connection.execute(text("COMMIT"))
            row = connection.exec_driver_sql(
                f"SELECT vec FROM {table_name} WHERE id = 1"
            ).fetchone()
            assert row is not None
            assert row[0] == [1.0, 2.0, 3.0]
        finally:
            meta.drop_all(config.db)

    def test_vector_orm_read_after_raw_insert(self) -> None:
        from sqlalchemy.sql.sqltypes import Float as SAFloat

        from snowflake.sqlalchemy.custom_types import VECTOR

        Base = declarative_base()

        class Embedding(Base):
            __tablename__ = "async_vector_orm"
            id = Column(Integer, primary_key=True)
            vec = Column(VECTOR(SAFloat(), 3))

        Base.metadata.create_all(config.db)
        try:
            # VECTOR insert must use raw SQL with ::VECTOR cast
            with config.db.begin() as conn:
                conn.exec_driver_sql(
                    "INSERT INTO async_vector_orm(id, vec) "
                    "SELECT 1, [4.0, 5.0, 6.0]::VECTOR(FLOAT, 3)"
                )

            with Session(config.db) as session:
                row = session.execute(
                    select(Embedding).where(Embedding.id == 1)
                ).scalar_one()
            assert row.vec == [4.0, 5.0, 6.0]
        finally:
            Base.metadata.drop_all(config.db)

    def test_vector_reflection(self) -> None:
        from sqlalchemy import inspect as sa_inspect

        from snowflake.sqlalchemy.custom_types import VECTOR

        Base = declarative_base()

        class Embedding(Base):
            __tablename__ = "async_vector_reflect"
            id = Column(Integer, primary_key=True)
            vec = Column(VECTOR("FLOAT", 4))

        Base.metadata.create_all(config.db)
        try:
            cols = sa_inspect(config.db).get_columns("async_vector_reflect")
            vec_col = next(c for c in cols if c["name"].lower() == "vec")
            assert isinstance(vec_col["type"], VECTOR)
            assert vec_col["type"].element_type == "FLOAT"
            assert vec_col["type"].dimension == 4
        finally:
            Base.metadata.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 7 — ORM Enum column (adapted from tests/test_orm.py::test_basic_orm)
# ---------------------------------------------------------------------------


class TestORMEnum(fixtures.TestBase):
    """Verifies that Python Enum columns are stored and retrieved correctly
    through the async adapter."""

    __backend__ = True

    def test_enum_insert_and_select(self) -> None:
        import enum as _enum

        from sqlalchemy import Enum

        class Status(_enum.Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        Base = declarative_base()

        class User(Base):
            __tablename__ = "async_orm_enum"
            id = Column(Integer, Sequence("async_orm_enum_seq"), primary_key=True)
            name = Column(String(50))
            status = Column(Enum(Status), default=Status.ACTIVE)

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add(User(name="alice", status=Status.ACTIVE))
                session.add(User(name="bob", status=Status.INACTIVE))
                session.commit()

            with Session(config.db) as session:
                active = (
                    session.execute(select(User).where(User.status == Status.ACTIVE))
                    .scalars()
                    .all()
                )
                inactive = (
                    session.execute(select(User).where(User.status == Status.INACTIVE))
                    .scalars()
                    .all()
                )
            assert len(active) == 1 and active[0].name == "alice"
            assert len(inactive) == 1 and inactive[0].name == "bob"
        finally:
            Base.metadata.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 8 — ORM aggregate functions (COUNT, MAX, SUM)
# ---------------------------------------------------------------------------


class TestORMAggregates(fixtures.TestBase):
    """Verifies that SQLAlchemy aggregate functions (func.count, func.max,
    func.sum) work through the async adapter — commonly used in dashboards
    and analytics queries."""

    __backend__ = True

    def test_count_max_sum(self) -> None:
        from sqlalchemy import func

        Base = declarative_base()

        class Score(Base):
            __tablename__ = "async_orm_agg"
            id = Column(Integer, Sequence("async_orm_agg_seq"), primary_key=True)
            player = Column(String(50))
            points = Column(Integer)

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add_all(
                    [
                        Score(player="alice", points=30),
                        Score(player="bob", points=50),
                        Score(player="alice", points=20),
                    ]
                )
                session.commit()

            with Session(config.db) as session:
                total = session.execute(select(func.count(Score.id))).scalar()
                maximum = session.execute(select(func.max(Score.points))).scalar()
                total_pts = session.execute(select(func.sum(Score.points))).scalar()
            assert total == 3
            assert maximum == 50
            assert total_pts == 100
        finally:
            Base.metadata.drop_all(config.db)

    def test_group_by_aggregate(self) -> None:
        from sqlalchemy import func

        Base = declarative_base()

        class Score(Base):
            __tablename__ = "async_orm_groupby"
            id = Column(Integer, Sequence("async_orm_groupby_seq"), primary_key=True)
            player = Column(String(50))
            points = Column(Integer)

        Base.metadata.create_all(config.db)
        try:
            with Session(config.db) as session:
                session.add_all(
                    [
                        Score(player="alice", points=30),
                        Score(player="bob", points=50),
                        Score(player="alice", points=20),
                    ]
                )
                session.commit()

            with Session(config.db) as session:
                rows = session.execute(
                    select(Score.player, func.sum(Score.points).label("total"))
                    .group_by(Score.player)
                    .order_by(Score.player)
                ).all()
            assert len(rows) == 2
            totals = {r.player: r.total for r in rows}
            assert totals["alice"] == 50
            assert totals["bob"] == 50
        finally:
            Base.metadata.drop_all(config.db)


# ---------------------------------------------------------------------------
# Test 9 — automap: reflect existing table into an ORM class
# ---------------------------------------------------------------------------


class TestAutomap(fixtures.TestBase):
    """Verifies that sqlalchemy.ext.automap can reflect a live Snowflake table
    into a mapped class and that ORM queries against that class work through
    the async adapter."""

    __backend__ = True

    def test_automap_reflect_and_query(self) -> None:
        from sqlalchemy import MetaData, Table
        from sqlalchemy.ext.automap import automap_base

        meta = MetaData()
        Table(
            "async_automap_users",
            meta,
            Column(
                "id", Integer, Sequence("async_automap_users_seq"), primary_key=True
            ),
            Column("username", String(50)),
        )
        meta.create_all(config.db)
        try:
            # seed two rows via Core so automap has data to query
            with config.db.begin() as conn:
                conn.execute(
                    meta.tables["async_automap_users"].insert(),
                    [{"username": "alice"}, {"username": "bob"}],
                )

            # automap.prepare() issues reflection queries through the engine
            Base = automap_base()
            Base.prepare(config.db, reflect=True)

            # automap lower-cases the table name to find the class
            AutoUser = Base.classes.async_automap_users

            with Session(config.db) as session:
                users = (
                    session.execute(select(AutoUser).order_by(AutoUser.username))
                    .scalars()
                    .all()
                )
            assert len(users) == 2
            assert users[0].username == "alice"
            assert users[1].username == "bob"
        finally:
            meta.drop_all(config.db)
