#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
"""Tests for the unit/integ auto-classification in ``tests/conftest.py``."""

import pytest

from tests.conftest import CONNECTION_FIXTURES, _is_integration


@pytest.mark.parametrize("fixture", sorted(CONNECTION_FIXTURES))
def test_connection_fixture_marks_integration(fixture):
    assert _is_integration(["request", fixture], "tests/test_core.py", False)


def test_offline_fixtures_mark_unit():
    assert not _is_integration(
        ["request", "sql_compiler", "snapshot"], "tests/test_compiler.py", False
    )


def test_explicit_integ_marker_wins():
    assert _is_integration(["sql_compiler"], "tests/test_compiler.py", True)


@pytest.mark.parametrize(
    "path",
    [
        "tests/sqlalchemy_test_suite/test_suite.py",
        "tests/alembic_integration/test_multi_schema_fk.py",
    ],
)
def test_live_only_directories_mark_integration(path):
    assert _is_integration([], path, False)


def test_this_module_is_classified_unit(request):
    assert request.node.get_closest_marker("unit") is not None
    assert request.node.get_closest_marker("integ") is None


def test_unit_test_does_not_create_schema(request):
    # The session schema must only be set up for integration tests, so offline
    # tests can run without credentials.
    assert "init_test_schema" not in request.fixturenames
