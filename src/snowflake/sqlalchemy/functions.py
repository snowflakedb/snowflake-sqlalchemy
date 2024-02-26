#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.

import warnings

from sqlalchemy.sql import functions as sqlfunc


class flatten(sqlfunc.GenericFunction):
    name = "FLATTEN"

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "For backward compatibility params are not rendered",
            SyntaxWarning,
            stacklevel=2,
        )
        return super().__init__(*args, **kwargs)
