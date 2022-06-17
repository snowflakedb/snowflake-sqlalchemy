#!/bin/bash -e
# Test Snowflake SQLAlchemy in Docker
# NOTES:
#   - By default this script runs Python 3.7 tests, as these are installed in dev vms
#   - To compile only a specific version(s) pass in versions like: `./test_docker.sh "3.7 3.8"`

set -o pipefail

# In case this is ran from dev-vm
PYTHON_ENV=${1:-3.7}

# Set constants
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SQLALCHEMY_DIR="$( dirname "${THIS_DIR}")"
WORKSPACE=${WORKSPACE:-$SQLALCHEMY_DIR}
source $THIS_DIR/set_base_image.sh

cd $THIS_DIR/docker/sqlalchemy_build

CONTAINER_NAME=test_sqlalchemy

echo "[Info] Building docker image"
arch=$(uname -p)

echo "[Info] Building docker image"
if [[ "$arch" == "aarch64" ]]; then
  BASE_IMAGE=$BASE_IMAGE_MANYLINUX2014AARCH64
  GOSU_URL=https://github.com/tianon/gosu/releases/download/1.14/gosu-arm64
else
  BASE_IMAGE=$BASE_IMAGE_MANYLINUX2014
  GOSU_URL=https://github.com/tianon/gosu/releases/download/1.14/gosu-amd64
fi

echo "[Info] Start building docker image and testing"

user_id=$(id -u ${USER})
docker run \
    --rm \
    --network=host \
    -e TERM=vt102 \
    -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
    -e OPENSSL_FIPS=1 \
    -e LOCAL_USER_ID=${user_id} \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e SF_REGRESS_LOGS \
    -e SF_PROJECT_ROOT \
    -e cloud_provider \
    -e JENKINS_HOME \
    -e GITHUB_ACTIONS \
    --mount type=bind,source="${SQLALCHEMY_DIR}",target=/home/user/snowflake-sqlalchemy \
    $(docker build --pull --build-arg BASE_IMAGE=$BASE_IMAGE --build-arg GOSU_URL="$GOSU_URL" -q .) \
    /home/user/snowflake-sqlalchemy/ci/test_linux.sh ${PYTHON_ENV}
