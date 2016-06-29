import pytest
from sqlalchemy import (Table, Column, Integer, String, MetaData, Sequence)
from sqlalchemy import inspect


def test_table_name_with_reserved_words(engine_testaccount):
    metadata = MetaData()
    insert_table = Table('insert', metadata,
                         Column('id', Integer, Sequence('insert_id_seq'),
                                primary_key=True),
                         Column('name', String),
                         Column('fullname', String),
                         )

    metadata.create_all(engine_testaccount)
    try:
        inspector = inspect(engine_testaccount)
        columns_in_insert = inspector.get_columns('insert')
        assert len(columns_in_insert) == 3
        assert columns_in_insert[0]['autoincrement'], 'autoinrecment'
        assert columns_in_insert[0]['default'] is None, 'default'
        assert columns_in_insert[0]['name'] == 'id', 'name'
        assert columns_in_insert[0]['primary_key'], 'primary key'
        assert not columns_in_insert[0]['nullable']

        columns_in_insert = inspector.get_columns('insert', schema='testschema')
        assert len(columns_in_insert) == 3

    except Exception as e:
        pytest.fail(e, pytest=True)
    finally:
        insert_table.drop(engine_testaccount)
    return insert_table
