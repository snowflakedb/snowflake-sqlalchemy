#!/bin/bash -e
#
# Build snowflake-sqlalchemy
set -o pipefail

PYTHON="python3.7"
THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQLALCHEMY_DIR="$(dirname "${THIS_DIR}")"
DIST_DIR="${SQLALCHEMY_DIR}/dist"

cd "$SQLALCHEMY_DIR"
# Clean up previously built DIST_DIR
if [ -d "${DIST_DIR}" ]; then
    echo "[WARN] ${DIST_DIR} already existing, deleting it..."
    rm -rf "${DIST_DIR}"
fi

# Constants and setup

echo "[Info] Building snowflake-sqlalchemy with $PYTHON"
# Clean up possible build artifacts
rm -rf build generated_version.py
${PYTHON} -m pip install --upgrade pip setuptools wheel build
${PYTHON} -m build --outdir ${DIST_DIR} .
