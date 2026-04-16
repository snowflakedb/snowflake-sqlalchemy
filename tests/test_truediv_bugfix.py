#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Test for visit_truediv_binary backward compatibility fix."""

import warnings

import pytest
from sqlalchemy import Integer, Numeric, column, literal, select

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


@pytest.mark.feature_v20
class TestTrueDivBackwardCompatibility:
    """Tests for the visit_truediv_binary backward compatibility fix."""

    def test_truediv_with_numeric_columns_div_is_floordiv_true(self):
        """Numeric columns should also get CAST when div_is_floordiv=True."""
        col1 = column("col1", Numeric)
        col2 = column("col2", Numeric)
        stmt = col1 / col2

        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        assert result == "col1 / CAST(col2 AS NUMERIC)"

    def test_truediv_with_literal_values(self):
        """Literal values should be handled correctly."""
        stmt = literal(5) / literal(2)

        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        assert "CAST" in result and "NUMERIC" in result

    def test_truediv_nested_in_select(self):
        """Division in SELECT statement should work correctly."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = select(col1 / col2)

        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        assert "col1 / CAST(col2 AS NUMERIC)" in result

    def test_floordiv_unchanged(self):
        """Floor division behavior should remain unchanged."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = col1 // col2

        # With div_is_floordiv=True (default) - delegates to parent which returns plain division
        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        assert result == "col1 / col2"

        # With div_is_floordiv=False - adds FLOOR
        result = str(
            stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False))
        )
        assert result == "FLOOR(col1 / col2)"

    def test_deprecation_warning_emitted(self):
        """Deprecation warning should be emitted when div_is_floordiv=True."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = col1 / col2

        # Should emit warning with default settings
        with pytest.warns(
            PendingDeprecationWarning,
            match="div_is_floordiv value will be changed to False",
        ):
            stmt.compile(dialect=SnowflakeDialect())

        # Should NOT emit warning when div_is_floordiv=False
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False))
        deprecation_warnings = [
            w for w in warning_list if issubclass(w.category, PendingDeprecationWarning)
        ]
        assert len(deprecation_warnings) == 0
