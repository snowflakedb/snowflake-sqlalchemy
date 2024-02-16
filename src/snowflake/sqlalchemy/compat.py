#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
from __future__ import annotations

import functools
import warnings
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
                warnings.warn(
                    "This needs refactoring", DeprecationWarning, stacklevel=2
                )
            fn(*reduced_args)

        return wrapper

    return fn_wrapper


def args_filler(positions_to_insert: tuple):
    """This method inserts into args values at positions.

    positions_to_insert is a tuple of tuple containing pair of posiotion and a value.
    ((6, False), (7, 42))
    It will insert at position 6 value False, and at positoin 6 value 42.
    """

    def fn_wrapper(fn: Callable):
        @functools.wraps(fn)
        def wrapper(*args):
            extended_args = list(args)
            if IS_VERSION_20:
                for idx, value in positions_to_insert:
                    extended_args.insert(idx, value)
                warnings.warn(
                    "This needs refactoring", DeprecationWarning, stacklevel=2
                )
            fn(*extended_args)

        return wrapper

    return fn_wrapper
