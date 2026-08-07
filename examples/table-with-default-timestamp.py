# courtesy of sfc-gh-mraba
from sqlalchemy import (
    Column,
    Integer,
    String,
    func,
)
from sqlalchemy.orm import Session, declarative_base
from snowflake.sqlalchemy import TIMESTAMP_NTZ

Base = declarative_base()

class TWTS(Base):
    __tablename__ = "table_with_timestamp"

    pk = Column(Integer, primary_key=True)
    name = Column(String(30))
    created = Column(TIMESTAMP_NTZ, server_default=func.now())

    def __repr__(self) -> str:
        return f"TWTS({self.pk=}, {self.name=}, {self.created=})"

Base.metadata.create_all(engine_testaccount)

session = Session(bind=engine_testaccount)
r1 = TWTS(pk=1, name="edward")
r2 = TWTS(pk=2, name="eddy")
assert r1.created is None
assert r2.created is None

session.add(r1)
session.add(r2)
session.commit()

rows = session.query(TWTS).all()
assert len(rows) == 2
for row in rows:
    print(row)
