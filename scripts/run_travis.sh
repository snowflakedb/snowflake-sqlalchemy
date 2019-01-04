#!/bin/bash -e
#
# Run Travis Tests
#
set -o pipefail
source ./venv/bin/activate
ret=0
timeout -s SIGUSR1 3600s py.test -vvv --cov=snowflake.sqlalchemy test || ret=$?
# TIMEOUT or SUCCESS
[ $ret != 124 -a $ret != 0 ] && exit 1 || exit 0
