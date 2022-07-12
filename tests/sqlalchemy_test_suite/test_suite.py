#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
import pytest
from sqlalchemy import Integer, testing
from sqlalchemy.schema import Column, Sequence, Table
from sqlalchemy.testing import config
from sqlalchemy.testing.suite import DifficultParametersTest as _DifficultParametersTest
from sqlalchemy.testing.suite import EscapingTest as _EscapingTest
from sqlalchemy.testing.suite import ExceptionTest as _ExceptionTest
from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
from sqlalchemy.testing.suite import HasSequenceTest as _HasSequenceTest
from sqlalchemy.testing.suite import (
    IdentityAutoincrementTest as _IdentityAutoincrementTest,
)
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import LikeFunctionsTest as _LikeFunctionsTest
from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
from sqlalchemy.testing.suite import PercentSchemaNamesTest as _PercentSchemaNamesTest
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import TextTest as _TextTest
from sqlalchemy.testing.suite import *  # noqa

# 1. Unsupported by snowflake db

del ComponentReflectionTest  # require indexes not supported by snowflake
del HasIndexTest  # require indexes not supported by snowflake
del QuotedNameArgumentTest  # require indexes not supported by snowflake
del ComputedReflectionTest  # expression not GA yet, SNOW-169530


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


# 3. Need further investigation, either to be skipped/removed by design, or to be fixed


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


# 4. Need fix in connector


class PercentSchemaNamesTest(_PercentSchemaNamesTest):
    @pytest.mark.xfail
    # TODO: connector cursor "executemany" needs to handle double percentage like
    #  "execute" using self._dbapi_connection._interpolate_empty_sequences
    def test_executemany_roundtrip(self, connection):
        super().test_executemany_roundtrip(connection)

    @pytest.mark.xfail
    # TODO: this failure is weird, running in standalone mode (just the method or PercentSchemaNamesTest) won't fail,
    #  however, running within the whole test_suite.py fails
    def test_single_roundtrip(self, connection):
        super().test_single_roundtrip(connection)


# 5. Patched Tests


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


class LikeFunctionsTest(_LikeFunctionsTest):
    @testing.requires.regexp_match
    @testing.combinations(
        ("a.cde.*", {1, 5, 6, 9}),
        ("abc.*", {1, 5, 6, 9, 10}),
        ("^abc.*", {1, 5, 6, 9, 10}),
        (".*9cde.*", {8}),
        ("^a.*", set(range(1, 11))),
        (".*(b|c).*", set(range(1, 11))),
        ("^(b|c).*", set()),
    )
    def test_regexp_match(self, text, expected):
        super().test_regexp_match(text, expected)

    def test_not_regexp_match(self):
        col = self.tables.some_table.c.data
        self._test(~col.regexp_match("a.cde.*"), {2, 3, 4, 7, 8, 10})
