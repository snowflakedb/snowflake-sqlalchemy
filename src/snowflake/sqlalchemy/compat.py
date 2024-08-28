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

from __future__ import annotations

import functools
from typing import Callable

from sqlalchemy import __version__ as SA_VERSION
from sqlalchemy import util

string_types = (str,)
returns_unicode = util.symbol("RETURNS_UNICODE")

IS_VERSION_20 = tuple(int(v) for v in SA_VERSION.split(".")) >= (2, 0, 0)


def args_reducer(positions_to_drop: tuple):
    """Removes args at positions provided in tuple positions_to_drop.

    For example tuple (3, 5) will remove items at third and fifth position.
    Keep in mind that on class methods first postion is cls or self.
    """

    def fn_wrapper(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args):
            reduced_args = args
            if not IS_VERSION_20:
                reduced_args = tuple(
                    arg for idx, arg in enumerate(args) if idx not in positions_to_drop
                )
            fn(*reduced_args)

        return wrapper

    return fn_wrapper
