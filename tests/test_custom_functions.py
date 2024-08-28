# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from sqlalchemy import func

from snowflake.sqlalchemy import snowdialect


def test_flatten_does_not_render_params():
    """This behavior is for backward compatibility.

    In previous version params were not rendered.
    In future this behavior will change.
    """
    flat = func.flatten("[1, 2]", outer=True)
    res = flat.compile(dialect=snowdialect.dialect())

    assert str(res) == "flatten(%(flatten_1)s)"


def test_flatten_emits_warning():
    expected_warning = "For backward compatibility params are not rendered."
    with pytest.warns(DeprecationWarning, match=expected_warning):
        func.flatten().compile(dialect=snowdialect.dialect())
