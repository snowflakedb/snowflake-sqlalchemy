#!/bin/bash -e
#
# Test Snowflake SQLAlchemy
#
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SQLALCHEMY_DIR="$( dirname "${THIS_DIR}")"
PARAMETERS_DIR="${SQLALCHEMY_DIR}/.github/workflows/parameters"

cd "${SQLALCHEMY_DIR}"

# Check Requirements
if [ -z "${PARAMETERS_SECRET}" ]; then
    echo "Missing PARAMETERS_SECRET, failing..."
    exit 1
fi

# Decrypt parameters file
PARAMS_FILE="${PARAMETERS_DIR}/parameters_aws.py.gpg"
[ ${cloud_provider} == azure ] && PARAMS_FILE="${PARAMETERS_DIR}/parameters_azure.py.gpg"
[ ${cloud_provider} == gcp ] && PARAMS_FILE="${PARAMETERS_DIR}/parameters_gcp.py.gpg"
gpg --quiet --batch --yes --decrypt --passphrase="${PARAMETERS_SECRET}" ${PARAMS_FILE} > tests/parameters.py

# Download artifacts made by build
aws s3 cp --recursive --only-show-errors s3://sfc-eng-jenkins/repository/sqlalchemy/linux/${client_git_branch}/${client_git_commit}/ dist

echo "[Info] Going to run regular tests for Python ${python_env}"
${THIS_DIR}/test_docker.sh ${python_env}
