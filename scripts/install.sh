#!/bin/bash -e
#
# Install Snowflake SQLAlchemy dialect
#
set -o pipefail
sudo apt-get update
openssl aes-256-cbc -k "$super_secret_password" -in parameters.py.enc -out test/parameters.py -d
curl -O https://bootstrap.pypa.io/get-pip.py
python get-pip.py
pip --version
pip install -U virtualenv
python -m virtualenv venv
source ./venv/bin/activate
if [[ "$TRAVIS_PYTHON_VERSION" == "3.4" ]]; then
    # last pandas supporting Python 3.4
    pip install pandas==0.20.3
else
    pip install pandas
fi

pip install pytest pytest-cov pytest-rerunfailures
if [[ "$TRAVIS_PYTHON_VERSION" == "2.7" ]]; then
    pip install mock
fi
pip install .
pip list --format=columns
