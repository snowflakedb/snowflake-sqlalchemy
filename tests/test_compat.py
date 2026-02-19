#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import importlib

import pytest

import snowflake.sqlalchemy.compat as compat_module


@pytest.fixture(scope="session", autouse=True)
def init_test_schema():
    """Neutralize the fixture that requires a live db connection."""
    yield


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.0.0", True),
        ("3.1.0", True),
        ("1.3.0", False),
        ("2.0.5.post1", True),
        ("2.0.0rc2", True),
        ("2.0.0b1", True),
        ("0.5.0beta3", False),
        ("0.4.2a", False),
    ],
)
def test_is_version_20(version, expected):
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("sqlalchemy.__version__", version)
        importlib.reload(compat_module)
        assert compat_module.IS_VERSION_20 == expected
    # Context manager reverted sqlalchemy.__version__; reload so
    # IS_VERSION_20 reflects the real version for later tests.
    importlib.reload(compat_module)
