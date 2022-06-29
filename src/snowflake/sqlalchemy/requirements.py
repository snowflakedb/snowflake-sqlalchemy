#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements

# TODO: check through the requirement list to determine which should be turned on/off


class Requirements(SuiteRequirements):
    @property
    def autocommit(self):
        return exclusions.open()

    @property
    def ctes(self):
        return exclusions.open()

    @property
    def ctes_on_dml(self):
        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        return exclusions.open()

    @property
    def delete_from(self):
        return exclusions.open()
