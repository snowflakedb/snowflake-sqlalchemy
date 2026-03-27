#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

"""Test for visit_truediv_binary backward compatibility fix."""

import pytest
from sqlalchemy import Integer, Numeric, column, func, literal, select

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect


class TestTrueDivBackwardCompatibility:
    """Tests for the visit_truediv_binary backward compatibility fix."""

    def test_truediv_with_div_is_floordiv_true_integer_columns(self):
        """When div_is_floordiv=True, integer division should add CAST."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = col1 / col2

        # Default behavior (div_is_floordiv=True)
        with pytest.warns(
            PendingDeprecationWarning, match="div_is_floordiv value will be changed"
        ):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        assert result == "col1 / CAST(col2 AS NUMERIC)"

    def test_truediv_with_div_is_floordiv_false_integer_columns(self):
        """When div_is_floordiv=False, integer division should not add CAST."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = col1 / col2

        # Explicit False - no CAST needed
        result = str(
            stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False))
        )
        assert result == "col1 / col2"

    def test_truediv_with_numeric_columns_div_is_floordiv_true(self):
        """Numeric columns should also get CAST when div_is_floordiv=True."""
        col1 = column("col1", Numeric)
        col2 = column("col2", Numeric)
        stmt = col1 / col2

        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        # Parent implementation adds CAST for consistency
        assert "CAST" in result and "NUMERIC" in result

    def test_truediv_with_expression_denominator(self):
        """Expressions as denominator should also get CAST when needed."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = col1 / func.sqrt(col2)

        # With div_is_floordiv=True (default)
        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        assert result == "col1 / CAST(sqrt(col2) AS NUMERIC)"

        # With div_is_floordiv=False
        result = str(
            stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False))
        )
        assert result == "col1 / sqrt(col2)"

    def test_truediv_with_literal_values(self):
        """Literal values should be handled correctly."""
        stmt = literal(5) / literal(2)

        # With div_is_floordiv=True (default)
        with pytest.warns(PendingDeprecationWarning):
            result = str(stmt.compile(dialect=SnowflakeDialect()))
        # Literals get bound parameters, but CAST should be present
        assert "CAST" in result and "NUMERIC" in result

    def test_truediv_nested_in_select(self):
        """Division in SELECT statement should work correctly."""
        col1 = column("col1", Integer)
        col2 = column("col2", Integer)
        stmt = select(col1 / col2)

        # With div_is_floordiv=True (default)
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
        import warnings

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")
            stmt.compile(dialect=SnowflakeDialect(force_div_is_floordiv=False))
        # Filter for PendingDeprecationWarning
        deprecation_warnings = [
            w for w in warning_list if issubclass(w.category, PendingDeprecationWarning)
        ]
        assert len(deprecation_warnings) == 0


@pytest.mark.parametrize(
    "numerator,denominator,force_div_is_floordiv,expected",
    [
        # Integer columns with div_is_floordiv=True
        (column("a", Integer), column("b", Integer), True, "a / CAST(b AS NUMERIC)"),
        # Integer columns with div_is_floordiv=False
        (column("a", Integer), column("b", Integer), False, "a / b"),
        # Expression with div_is_floordiv=True
        (
            column("a", Integer),
            func.sqrt(column("b", Integer)),
            True,
            "a / CAST(sqrt(b) AS NUMERIC)",
        ),
        # Expression with div_is_floordiv=False
        (column("a", Integer), func.sqrt(column("b", Integer)), False, "a / sqrt(b)"),
    ],
)
def test_truediv_parametrized(numerator, denominator, force_div_is_floordiv, expected):
    """Parametrized test for various division scenarios."""
    stmt = numerator / denominator

    if force_div_is_floordiv:
        with pytest.warns(PendingDeprecationWarning):
            result = str(
                stmt.compile(
                    dialect=SnowflakeDialect(
                        force_div_is_floordiv=force_div_is_floordiv
                    )
                )
            )
    else:
        result = str(
            stmt.compile(
                dialect=SnowflakeDialect(force_div_is_floordiv=force_div_is_floordiv)
            )
        )

    assert result == expected
