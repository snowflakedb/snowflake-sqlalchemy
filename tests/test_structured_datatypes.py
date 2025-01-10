#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest
import sqlalchemy as sa
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    Sequence,
    Table,
    cast,
    exc,
    inspect,
    text,
)
from sqlalchemy.orm import Session, declarative_base
from sqlalchemy.sql import select
from sqlalchemy.sql.ddl import CreateTable

from snowflake.sqlalchemy import NUMBER, IcebergTable, SnowflakeTable
from snowflake.sqlalchemy.custom_types import ARRAY, MAP, OBJECT, TEXT
from snowflake.sqlalchemy.exc import StructuredTypeNotSupportedInTableColumnsError


@pytest.mark.parametrize(
    "structured_type",
    [
        MAP(NUMBER(10, 0), MAP(NUMBER(10, 0), TEXT(16777216))),
        OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
        OBJECT(key1=TEXT(16777216), key2=NUMBER(10, 0)),
        ARRAY(MAP(NUMBER(10, 0), TEXT(16777216))),
    ],
)
def test_compile_table_with_structured_data_type(
    sql_compiler, snapshot, structured_type
):
    metadata = MetaData()
    user_table = Table(
        "clustered_user",
        metadata,
        Column("Id", Integer, primary_key=True),
        Column("name", structured_type),
    )

    create_table = CreateTable(user_table)

    assert sql_compiler(create_table) == snapshot


def test_compile_table_with_sqlalchemy_array(sql_compiler, snapshot):
    metadata = MetaData()
    user_table = Table(
        "clustered_user",
        metadata,
        Column("Id", Integer, primary_key=True),
        Column("name", sa.ARRAY(sa.String)),
    )

    create_table = CreateTable(user_table)

    assert sql_compiler(create_table) == snapshot


@pytest.mark.requires_external_volume
def test_insert_map(engine_testaccount, external_volume, base_location, snapshot):
    metadata = MetaData()
    table_name = "test_insert_map"
    test_map = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("map_id", MAP(NUMBER(10, 0), TEXT(16777216))),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            slt = select(
                1,
                cast(
                    text("{'100':'item1', '200':'item2'}"),
                    MAP(NUMBER(10, 0), TEXT(16777216)),
                ),
            )
            ins = test_map.insert().from_select(["id", "map_id"], slt)
            conn.execute(ins)

            results = conn.execute(test_map.select())
            data = results.fetchmany()
            results.close()
            snapshot.assert_match(data)
    finally:
        test_map.drop(engine_testaccount)


@pytest.mark.requires_external_volume
def test_insert_map_orm(
    sql_compiler, external_volume, base_location, engine_testaccount, snapshot
):
    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestIcebergTableOrm(Base):
        __tablename__ = "test_iceberg_table_orm"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return IcebergTable(name, metadata, *arg, **kw)

        __table_args__ = {
            "external_volume": external_volume,
            "base_location": base_location,
        }

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        map_id = Column(MAP(NUMBER(10, 0), TEXT(16777216)))

        def __repr__(self):
            return f"({self.id!r}, {self.name!r})"

    Base.metadata.create_all(engine_testaccount)

    try:
        cast_expr = cast(
            text("{'100':'item1', '200':'item2'}"), MAP(NUMBER(10, 0), TEXT(16777216))
        )
        instance = TestIcebergTableOrm(id=0, map_id=cast_expr)
        session.add(instance)
        with pytest.raises(exc.ProgrammingError) as programming_error:
            session.commit()
        # TODO: Support variant in insert statement
        assert str(programming_error.value.orig) == snapshot
    finally:
        Base.metadata.drop_all(engine_testaccount)


@pytest.mark.requires_external_volume
def test_select_map_orm(engine_testaccount, external_volume, base_location, snapshot):
    metadata = MetaData()
    table_name = "test_select_map_orm"
    test_map = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("map_id", MAP(NUMBER(10, 0), TEXT(16777216))),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        slt1 = select(
            2,
            cast(
                text("{'100':'item1', '200':'item2'}"),
                MAP(NUMBER(10, 0), TEXT(16777216)),
            ),
        )
        slt2 = select(
            1,
            cast(
                text("{'100':'item1', '200':'item2'}"),
                MAP(NUMBER(10, 0), TEXT(16777216)),
            ),
        ).union_all(slt1)
        ins = test_map.insert().from_select(["id", "map_id"], slt2)
        conn.execute(ins)
        conn.commit()

    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestIcebergTableOrm(Base):
        __table__ = test_map

        def __repr__(self):
            return f"({self.id!r}, {self.map_id!r})"

    try:
        data = session.query(TestIcebergTableOrm).all()
        snapshot.assert_match(data)
    finally:
        test_map.drop(engine_testaccount)


@pytest.mark.requires_external_volume
def test_select_array_orm(engine_testaccount, external_volume, base_location, snapshot):
    metadata = MetaData()
    table_name = "test_select_array_orm"
    test_map = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("array_col", ARRAY(TEXT(16777216))),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        slt1 = select(
            2,
            cast(
                text("['item1','item2']"),
                ARRAY(TEXT(16777216)),
            ),
        )
        slt2 = select(
            1,
            cast(
                text("['item3','item4']"),
                ARRAY(TEXT(16777216)),
            ),
        ).union_all(slt1)
        ins = test_map.insert().from_select(["id", "array_col"], slt2)
        conn.execute(ins)
        conn.commit()

    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestIcebergTableOrm(Base):
        __table__ = test_map

        def __repr__(self):
            return f"({self.id!r}, {self.array_col!r})"

    try:
        data = session.query(TestIcebergTableOrm).all()
        snapshot.assert_match(data)
    finally:
        test_map.drop(engine_testaccount)


@pytest.mark.requires_external_volume
def test_insert_array(engine_testaccount, external_volume, base_location, snapshot):
    metadata = MetaData()
    table_name = "test_insert_map"
    test_map = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("array_col", ARRAY(TEXT(16777216))),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            slt = select(
                1,
                cast(
                    text("['item1','item2']"),
                    ARRAY(TEXT(16777216)),
                ),
            )
            ins = test_map.insert().from_select(["id", "array_col"], slt)
            conn.execute(ins)

            results = conn.execute(test_map.select())
            data = results.fetchmany()
            results.close()
            snapshot.assert_match(data)
    finally:
        test_map.drop(engine_testaccount)


@pytest.mark.requires_external_volume
def test_insert_array_orm(
    sql_compiler, external_volume, base_location, engine_testaccount, snapshot
):
    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestIcebergTableOrm(Base):
        __tablename__ = "test_iceberg_table_orm"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return IcebergTable(name, metadata, *arg, **kw)

        __table_args__ = {
            "external_volume": external_volume,
            "base_location": base_location,
        }

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        array_col = Column(ARRAY(TEXT(16777216)))

        def __repr__(self):
            return f"({self.id!r}, {self.name!r})"

    Base.metadata.create_all(engine_testaccount)

    try:
        cast_expr = cast(text("['item1','item2']"), ARRAY(TEXT(16777216)))
        instance = TestIcebergTableOrm(id=0, array_col=cast_expr)
        session.add(instance)
        with pytest.raises(exc.ProgrammingError) as programming_error:
            session.commit()
        # TODO: Support variant in insert statement
        assert str(programming_error.value.orig) == snapshot
    finally:
        Base.metadata.drop_all(engine_testaccount)


@pytest.mark.requires_external_volume
def test_insert_structured_object(
    engine_testaccount, external_volume, base_location, snapshot
):
    metadata = MetaData()
    table_name = "test_insert_structured_object"
    test_map = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "object_col",
            OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
        ),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    try:
        with engine_testaccount.connect() as conn:
            slt = select(
                1,
                cast(
                    text("{'key1':'item1', 'key2': 15}"),
                    OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
                ),
            )
            ins = test_map.insert().from_select(["id", "object_col"], slt)
            conn.execute(ins)

            results = conn.execute(test_map.select())
            data = results.fetchmany()
            results.close()
            snapshot.assert_match(data)
    finally:
        test_map.drop(engine_testaccount)


@pytest.mark.requires_external_volume
def test_insert_structured_object_orm(
    sql_compiler, external_volume, base_location, engine_testaccount, snapshot
):
    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestIcebergTableOrm(Base):
        __tablename__ = "test_iceberg_table_orm"

        @classmethod
        def __table_cls__(cls, name, metadata, *arg, **kw):
            return IcebergTable(name, metadata, *arg, **kw)

        __table_args__ = {
            "external_volume": external_volume,
            "base_location": base_location,
        }

        id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
        object_col = Column(
            OBJECT(key1=(NUMBER(10, 0), False), key2=(TEXT(16777216), False))
        )

        def __repr__(self):
            return f"({self.id!r}, {self.name!r})"

    Base.metadata.create_all(engine_testaccount)

    try:
        cast_expr = cast(
            text("{ 'key1' : 1, 'key2' : 'item1' }"),
            OBJECT(key1=(NUMBER(10, 0), False), key2=(TEXT(16777216), False)),
        )
        instance = TestIcebergTableOrm(id=0, object_col=cast_expr)
        session.add(instance)
        with pytest.raises(exc.ProgrammingError) as programming_error:
            session.commit()
        # TODO: Support variant in insert statement
        assert str(programming_error.value.orig) == snapshot
    finally:
        Base.metadata.drop_all(engine_testaccount)


@pytest.mark.requires_external_volume
def test_select_structured_object_orm(
    engine_testaccount, external_volume, base_location, snapshot
):
    metadata = MetaData()
    table_name = "test_select_structured_object_orm"
    iceberg_table = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column(
            "structured_obj_col",
            OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
        ),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    with engine_testaccount.connect() as conn:
        first_select = select(
            2,
            cast(
                text("{'key1': 'value1', 'key2': 1}"),
                OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
            ),
        )
        second_select = select(
            1,
            cast(
                text("{'key1': 'value2', 'key2': 2}"),
                OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
            ),
        ).union_all(first_select)
        insert_statement = iceberg_table.insert().from_select(
            ["id", "structured_obj_col"], second_select
        )
        conn.execute(insert_statement)
        conn.commit()

    Base = declarative_base()
    session = Session(bind=engine_testaccount)

    class TestIcebergTableOrm(Base):
        __table__ = iceberg_table

        def __repr__(self):
            return f"({self.id!r}, {self.structured_obj_col!r})"

    try:
        data = session.query(TestIcebergTableOrm).all()
        snapshot.assert_match(data)
    finally:
        iceberg_table.drop(engine_testaccount)


@pytest.mark.requires_external_volume
@pytest.mark.parametrize(
    "structured_type, expected_type",
    [
        (MAP(NUMBER(10, 0), TEXT(16777216)), MAP),
        (MAP(NUMBER(10, 0), MAP(NUMBER(10, 0), TEXT(16777216))), MAP),
        (
            OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
            OBJECT,
        ),
        (ARRAY(TEXT(16777216)), ARRAY),
    ],
)
def test_inspect_structured_data_types(
    engine_testaccount,
    external_volume,
    base_location,
    snapshot,
    structured_type,
    expected_type,
):
    metadata = MetaData()
    table_name = "test_st_types"
    test_map = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("structured_type_col", structured_type),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)

    try:
        inspecter = inspect(engine_testaccount)
        columns = inspecter.get_columns(table_name)

        assert isinstance(columns[0]["type"], NUMBER)
        assert isinstance(columns[1]["type"], expected_type)
        assert columns == snapshot

    finally:
        test_map.drop(engine_testaccount)


@pytest.mark.requires_external_volume
@pytest.mark.parametrize(
    "structured_type",
    [
        "MAP(NUMBER(10, 0), VARCHAR)",
        "MAP(NUMBER(10, 0), MAP(NUMBER(10, 0), VARCHAR))",
        "OBJECT(key1 VARCHAR, key2 NUMBER(10, 0))",
        "ARRAY(MAP(NUMBER(10, 0), VARCHAR))",
    ],
)
def test_reflect_structured_data_types(
    engine_testaccount,
    external_volume,
    base_location,
    snapshot,
    structured_type,
    sql_compiler,
):
    metadata = MetaData()
    table_name = "test_reflect_st_types"
    create_table_sql = f"""
CREATE OR REPLACE ICEBERG TABLE {table_name} (
       id number(38,0) primary key,
       structured_type_col {structured_type})
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = '{external_volume}'
BASE_LOCATION = '{base_location}';
      """

    with engine_testaccount.connect() as connection:
        connection.exec_driver_sql(create_table_sql)

    iceberg_table = IcebergTable(table_name, metadata, autoload_with=engine_testaccount)
    constraint = iceberg_table.constraints.pop()
    constraint.name = "constraint_name"
    iceberg_table.constraints.add(constraint)

    try:
        with engine_testaccount.connect():
            value = CreateTable(iceberg_table)

            actual = sql_compiler(value)

            assert actual == snapshot

    finally:
        metadata.drop_all(engine_testaccount)


@pytest.mark.requires_external_volume
def test_create_table_structured_datatypes(
    engine_testaccount, external_volume, base_location
):
    metadata = MetaData()
    table_name = "test_structured0"
    test_structured_dt = IcebergTable(
        table_name,
        metadata,
        Column("id", Integer, primary_key=True),
        Column("map_id", MAP(NUMBER(10, 0), TEXT(16777216))),
        Column(
            "object_col",
            OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
        ),
        Column(
            "array_col",
            ARRAY(TEXT(16777216)),
        ),
        external_volume=external_volume,
        base_location=base_location,
    )
    metadata.create_all(engine_testaccount)
    try:
        assert test_structured_dt is not None
    finally:
        test_structured_dt.drop(engine_testaccount)


@pytest.mark.parametrize(
    "structured_type_col",
    [
        Column("name", MAP(NUMBER(10, 0), TEXT(16777216))),
        Column(
            "object_col",
            OBJECT(key1=(TEXT(16777216), False), key2=(NUMBER(10, 0), False)),
        ),
        Column("name", ARRAY(TEXT(16777216))),
    ],
)
def test_structured_type_not_supported_in_table_columns_error(
    sql_compiler, structured_type_col
):
    metadata = MetaData()
    with pytest.raises(
        StructuredTypeNotSupportedInTableColumnsError
    ) as programming_error:
        SnowflakeTable(
            "clustered_user",
            metadata,
            Column("Id", Integer, primary_key=True),
            structured_type_col,
        )
    assert programming_error is not None
