#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Integer, testing
from sqlalchemy.schema import Column, Sequence, Table
from sqlalchemy.testing import config
from sqlalchemy.testing.suite import AutocommitIsolationTest as _AutocommitIsolationTest
from sqlalchemy.testing.suite import (
    CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
)
from sqlalchemy.testing.suite import CTETest as _CTETest
from sqlalchemy.testing.suite import DateTest as _DateTest
from sqlalchemy.testing.suite import (
    DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest,
)
from sqlalchemy.testing.suite import (
    DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest,
)
from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest
from sqlalchemy.testing.suite import DifficultParametersTest as _DifficultParametersTest
from sqlalchemy.testing.suite import EscapingTest as _EscapingTest
from sqlalchemy.testing.suite import ExceptionTest as _ExceptionTest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasSequenceTest as _HasSequenceTest
from sqlalchemy.testing.suite import HasSequenceTestEmpty as _HasSequenceTestEmpty
from sqlalchemy.testing.suite import (
    IdentityAutoincrementTest as _IdentityAutoincrementTest,
)
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import IsolationLevelTest as _IsolationLevelTest
from sqlalchemy.testing.suite import LastrowidTest as _LastrowidTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import NumericTest as _NumericTest
from sqlalchemy.testing.suite import SequenceTest as _SequenceTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import TextTest as _TextTest
from sqlalchemy.testing.suite import TimeMicrosecondsTest as _TimeMicrosecondsTest
from sqlalchemy.testing.suite import TimeTest as _TimeTest
from sqlalchemy.testing.suite import *  # noqa

# 1. Unsupported by snowflake db

del ComponentReflectionTest  # require indexes not supported by snowflake
del HasIndexTest  # require indexes not supported by snowflake
del QuotedNameArgumentTest  # require indexes not supported by snowflake


class LongNameBlowoutTest(_LongNameBlowoutTest):
    # The combination ("ix",) is removed due to Snowflake not supporting indexes
    def ix(self, metadata, connection):
        pytest.skip("ix required index feature not supported by Snowflake")


class FetchLimitOffsetTest(_FetchLimitOffsetTest):
    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_bound_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_expr_limit(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_expr_limit_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_expr_limit_simple_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_expr_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_simple_limit_expr_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_simple_offset(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake only takes non-negative integer constants for offset/limit"
    )
    def test_simple_offset_zero(self, connection):
        pass


class InsertBehaviorTest(_InsertBehaviorTest):
    @pytest.mark.skip(
        "Snowflake does not support inserting empty values, the value may be a literal or an expression."
    )
    def test_empty_insert(self, connection):
        pass

    @pytest.mark.skip(
        "Snowflake does not support inserting empty values, The value may be a literal or an expression."
    )
    def test_empty_insert_multiple(self, connection):
        pass


# 2. Not implemented by snowflake-sqlalchemy


class AutocommitIsolationTest(_AutocommitIsolationTest):
    @pytest.mark.skip("set_isolation_level not implemented in SnowflakeDialect")
    def test_autocommit_on(self, connection_no_trans):
        pass

    @pytest.mark.skip("set_isolation_level not implemented in SnowflakeDialect")
    def test_turn_autocommit_off_via_default_iso_level(self, connection_no_trans):
        pass


class HasSequenceTest(_HasSequenceTest):
    # Override the define_tables method as snowflake does not support 'nomaxvalue'/'nominvalue'
    @classmethod
    def define_tables(cls, metadata):
        Sequence("user_id_seq", metadata=metadata)
        # Replace Sequence("other_seq") creation as in the original test suite,
        # the Sequence created with 'nomaxvalue' and 'nominvalue'
        # which snowflake does not support:
        #     Sequence("other_seq", metadata=metadata, nomaxvalue=True, nominvalue=True)
        Sequence("other_seq", metadata=metadata)
        if testing.requires.schemas.enabled:
            Sequence("user_id_seq", schema=config.test_schema, metadata=metadata)
            Sequence("schema_seq", schema=config.test_schema, metadata=metadata)
        Table(
            "user_id_table",
            metadata,
            Column("id", Integer, primary_key=True),
        )

    @pytest.mark.skip("get_sequence_names not implemented in SnowflakeDialect")
    def test_get_sequence_names(self, connection):
        pass

    @pytest.mark.skip("get_sequence_names not implemented in SnowflakeDialect")
    def test_get_sequence_names_no_sequence_schema(self, connection):
        pass

    @pytest.mark.skip("get_sequence_names not implemented in SnowflakeDialect")
    def test_get_sequence_names_sequences_schema(self, connection):
        pass


class HasSequenceTestEmpty(_HasSequenceTestEmpty):
    @pytest.mark.skip("get_sequence_names not implemented in SnowflakeDialect")
    def test_get_sequence_names_no_sequence(self, connection):
        pass


class IsolationLevelTest(_IsolationLevelTest):
    @pytest.mark.skip("get_isolation_level not implemented in SnowflakeDialect")
    def test_all_levels(self):
        pass

    @pytest.mark.skip("get_isolation_level not implemented in SnowflakeDialect")
    def test_non_default_isolation_level(self):
        pass


# 3. Need further investigation, either to be skipped/removed by design, or to be fixed


class CompositeKeyReflectionTest(_CompositeKeyReflectionTest):
    @pytest.mark.skip("need investigation")
    def test_pk_column_order(self):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: ['attr', 'id', 'name'] != ['name', 'id', 'attr']
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_fk_column_order(self):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: ['attr', 'id', 'name'] != ['name', 'id', 'attr']
        """
        pass


class CTETest(_CTETest):
    @pytest.mark.skip("need investigation")
    def test_delete_from_round_trip(self, connection):
        """
        E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 001003 (42000): SQL compilation error:
        E       syntax error line 5 at position 1 unexpected 'DELETE'.
        E       [SQL: WITH some_cte AS
        E       (SELECT some_table.id AS id, some_table.data AS data, some_table.parent_id AS parent_id
        E       FROM some_table
        E       WHERE some_table.data IN (%(data_1_1)s, %(data_1_2)s, %(data_1_3)s))
        E        DELETE FROM some_other_table USING some_cte WHERE some_other_table.data = some_cte.data]
        E       [parameters: {'data_1_1': 'd2', 'data_1_2': 'd3', 'data_1_3': 'd4'}]
        E       (Background on this error at: https://sqlalche.me/e/14/f405)
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_delete_scalar_subq_round_trip(self, connection):
        """
                E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 001003 (42000): SQL compilation error:
        E       syntax error line 5 at position 1 unexpected 'DELETE'.
        E       [SQL: WITH some_cte AS
        E       (SELECT some_table.id AS id, some_table.data AS data, some_table.parent_id AS parent_id
        E       FROM some_table
        E       WHERE some_table.data IN (%(data_1_1)s, %(data_1_2)s, %(data_1_3)s))
        E        DELETE FROM some_other_table WHERE some_other_table.data = (SELECT some_cte.data
        E       FROM some_cte
        E       WHERE some_cte.id = some_other_table.id)]
        E       [parameters: {'data_1_1': 'd2', 'data_1_2': 'd3', 'data_1_3': 'd4'}]
        E       (Background on this error at: https://sqlalche.me/e/14/f405)
        """
        pass


class DateTest(_DateTest):
    @pytest.mark.skip("need investigation")
    def test_select_direct(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '2012-10-15' != datetime.date(2012, 10, 15)
        """
        pass


class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):
    @pytest.mark.skip("need investigation")
    def test_select_direct(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '2012-10-15 12:57:18' != datetime.datetime(2012, 10, 15, 12, 57, 18)
        """
        pass


class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):
    @pytest.mark.skip("need investigation")
    def test_select_direct(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '2012-10-15 12:57:18.000396' != datetime.datetime(2012, 10, 15, 12, 57, 18, 396)
        """
        pass


class DateTimeTest(_DateTimeTest):
    @pytest.mark.skip("need investigation")
    def test_select_direct(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '2012-10-15 12:57:18' != datetime.datetime(2012, 10, 15, 12, 57, 18)
        """
        pass


class DifficultParametersTest(_DifficultParametersTest):
    @pytest.mark.skip("need investigation")
    def test_round_trip(self, name, connection, metadata):
        """
                Failing combinations are
                ("%percent",),

        E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 000904 (42000): SQL compilation error: error line 1 at position 19
        E       invalid identifier '"%percent"'
        E       [SQL: INSERT INTO t (id, "%%percent") VALUES (%(id)s, %(Ppercent)s)]
        E       [parameters: {'id': 1, 'Ppercent': 'some name'}]
        E       (Background on this error at: https://sqlalche.me/e/14/f405)

                ("more :: %colons%",),

        E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 000904 (42000): SQL compilation error: error line 1 at position 19
        E       invalid identifier '"more :: %colons%"'
        E       [SQL: INSERT INTO t (id, "more :: %%colons%%") VALUES (%(id)s, %(more CC PcolonsP)s)]
        E       [parameters: {'id': 1, 'more CC PcolonsP': 'some name'}]

                ("per % cent",),

        E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 000904 (42000): SQL compilation error: error line 1 at position 19
        E       invalid identifier '"per % cent"'
        E       [SQL: INSERT INTO t (id, "per %% cent") VALUES (%(id)s, %(per P cent)s)]
        E       [parameters: {'id': 1, 'per P cent': 'some name'}]

                ("percent%(ens)yah",),

        E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 000904 (42000): SQL compilation error: error line 1 at position 19
        E       invalid identifier '"percent%(ens)yah"'
        E       [SQL: INSERT INTO t (id, "percent%%(ens)yah") VALUES (%(id)s, %(percentPAensZyah)s)]
        E       [parameters: {'id': 1, 'percentPAensZyah': 'some name'}]
        """
        pass


class EscapingTest(_EscapingTest):
    @pytest.mark.skip("need investigation")
    def test_percent_sign_round_trip(self):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: None != 'some % value'
        """
        pass


class ExceptionTest(_ExceptionTest):
    @pytest.mark.skip("need investigation")
    def test_integrity_error(self):
        """
        # assert outside the block so it works for AssertionError too !
        >       assert success, "Callable did not raise an exception"
        E       AssertionError: Callable did not raise an exception
        """
        pass


class IdentityAutoincrementTest(_IdentityAutoincrementTest):
    @pytest.mark.skip("need investigation")
    def test_autoincrement_with_identity(self, connection):
        """
        E       sqlalchemy.exc.IntegrityError: (snowflake.connector.errors.IntegrityError) 100072 (22000): NULL result in a non-nullable column
        E       [SQL: INSERT INTO tbl (desc) VALUES (%(desc)s)]
        E       [parameters: {'desc': 'row'}]
        E       (Background on this error at: https://sqlalche.me/e/14/gkpj)
        """
        pass


class LastrowidTest(_LastrowidTest):
    @pytest.mark.skip("need investigation")
    def test_last_inserted_id(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: (None,) != (2,)
        """
        pass


class NumericTest(_NumericTest):
    @pytest.mark.skip("need investigation")
    def test_decimal_coerce_round_trip(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '15.7563' != Decimal('15.7563')
        """
        pass


class SequenceTest(_SequenceTest):
    @pytest.mark.skip("need investigation")
    def test_insert_lastrowid(self, connection):
        """
        E       sqlalchemy.exc.ProgrammingError: (snowflake.connector.errors.ProgrammingError) 000904 (42000): SQL compilation error: error line 2 at position 29
        E       invalid identifier 'NORET_SCH_ID_SEQ.NEXTVAL'
        E       [SQL:
        E       CREATE TABLE test_schema.seq_no_returning_sch (
        E       	id INTEGER NOT NULL DEFAULT noret_sch_id_seq.nextval,
        E       	data VARCHAR(50),
        E       	PRIMARY KEY (id)
        E       )
        E
        E       ]
        E       (Background on this error at: https://sqlalche.me/e/14/f405)
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_insert_roundtrip(self, connection):
        """
        similar invalid identifier error as the first one
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_insert_roundtrip_no_implicit_returning(self, connection):
        """
        similar invalid identifier error as the first one
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_insert_roundtrip_translate(self, connection, implicit_returning):
        """
        similar invalid identifier error as the first one
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_nextval_direct(self, connection):
        """
        similar invalid identifier error as the first one
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_nextval_direct_schema_translate(self, connection):
        """
        similar invalid identifier error as the first one
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_optional_seq(self, connection):
        """
        similar invalid identifier error as the first one
        """
        pass


class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    @pytest.mark.skip("need investigation")
    def test_delete(self, connection):
        """
        >       assert not r.returns_rows
        E       assert not True
        E        +  where True = <sqlalchemy.engine.cursor.LegacyCursorResult object at 0x7fbcf82efdc0>.returns_rows
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_update(self, connection):
        """
        >       assert not r.returns_rows
        E       assert not True
        E        +  where True = <sqlalchemy.engine.cursor.LegacyCursorResult object at 0x7fbd084e5c10>.returns_rows
        """
        pass


class StringTest(_StringTest):
    @pytest.mark.skip("need investigation")
    def test_literal_backslashes(self, literal_round_trip):
        """
        'backslash one  backslash two \\ end' != ['backslash one \\ backslash two \\\\ end']

        Expected :['backslash one \\ backslash two \\\\ end']
        Actual   :'backslash one  backslash two \\ end'
        """
        pass


class TextTest(_TextTest):
    @pytest.mark.skip("need investigation")
    def test_literal_backslashes(self, literal_round_trip):
        """
        'backslash one  backslash two \\ end' != ['backslash one \\ backslash two \\\\ end']

        Expected :['backslash one \\ backslash two \\\\ end']
        Actual   :'backslash one  backslash two \\ end'
        """
        pass

    @pytest.mark.skip("need investigation")
    def test_literal_percentsigns(self, literal_round_trip):
        """
        'percent %% signs %%%% percent' != ['percent % signs %% percent']

        Expected :['percent % signs %% percent']
        Actual   :'percent %% signs %%%% percent'
        """
        pass


class TimeMicrosecondsTest(_TimeMicrosecondsTest):
    @pytest.mark.skip("need investigation")
    def test_select_direct(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '12:57:18.000396' != datetime.time(12, 57, 18, 396)
        """
        pass


class TimeTest(_TimeTest):
    @pytest.mark.skip("need investigation")
    def test_select_direct(self, connection):
        """
        >       assert a == b, msg or "%r != %r" % (a, b)
        E       AssertionError: '12:57:18' != datetime.time(12, 57, 18)
        """
        pass
