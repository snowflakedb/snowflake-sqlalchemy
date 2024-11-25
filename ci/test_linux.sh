#!/bin/bash -e
#
# Test Snowflake SQLAlchemy in Linux
# NOTES:
#   - Versions to be tested should be passed in as the first argument, e.g: "3.7 3.8". If omitted 3.7-3.11 will be assumed.
#   - This script assumes that ../dist/repaired_wheels has the wheel(s) built for all versions to be tested
#   - This is the script that test_docker.sh runs inside of the docker container

PYTHON_VERSIONS="${1:-3.8 3.9 3.10 3.11}"
THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQLALCHEMY_DIR="$(dirname "${THIS_DIR}")"

# Install one copy of tox
python3 -m pip install -U tox

# Run tests
cd $SQLALCHEMY_DIR
for PYTHON_VERSION in ${PYTHON_VERSIONS}; do
  echo "[Info] Testing with ${PYTHON_VERSION}"
  SHORT_VERSION=$(python3 -c "print('${PYTHON_VERSION}'.replace('.', ''))")
  SQLALCHEMY_WHL=$(ls $SQLALCHEMY_DIR/dist/snowflake_sqlalchemy-*-py3-none-any.whl | sort -r | head -n 1)
  TEST_ENVLIST=fix_lint,py${SHORT_VERSION}-ci,py${SHORT_VERSION}-coverage
  echo "[Info] Running tox for ${TEST_ENVLIST}"
  python3 -m tox -e ${TEST_ENVLIST} --installpkg ${SQLALCHEMY_WHL}
done
