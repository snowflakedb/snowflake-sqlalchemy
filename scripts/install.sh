#!/bin/bash -e
#
# Install Snowflake SQLAlchemy dialect
#
set -o pipefail

SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$TRAVIS_OS_NAME" == "osx" ]; then
    curl -O https://www.python.org/ftp/python/${PYTHON_VERSION}/python-${PYTHON_VERSION}-macosx10.9.pkg
    sudo installer -pkg python-${PYTHON_VERSION}-macosx10.9.pkg -target /
    which python3
    python3 --version
    python3 -m venv venv
else
    sudo apt-get update
    pip install -U virtualenv
    python -m virtualenv venv
fi
if [[ -n "$SNOWFLAKE_AZURE" ]]; then
    openssl aes-256-cbc -k "$super_azure_secret_password" -in parameters_az.py.enc -out test/parameters.py -d
elif [[ -n "$SNOWFLAKE_GCP" ]]; then
    openssl aes-256-cbc -k "$super_gcp_secret_password" -in parameters_gcp.py.enc -out test/parameters.py -d
else
    openssl aes-256-cbc -k "$super_secret_password" -in parameters.py.enc -out test/parameters.py -d
fi

if [ "$TRAVIS_OS_NAME" != "osx" ]; then
    $SCRIPTS_DIR/../ci/wss.sh
fi
source ./venv/bin/activate
pip install '.[development]'
pip list --format=columns
