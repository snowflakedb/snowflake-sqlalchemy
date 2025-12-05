#!/bin/bash -e
#
# Build snowflake-sqlalchemy universal wheel in Docker
set -o pipefail

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source $THIS_DIR/set_base_image.sh
SQLALCHEMY_DIR="$( dirname "${THIS_DIR}")"

mkdir -p $SQLALCHEMY_DIR/dist
cd $THIS_DIR/docker/sqlalchemy_build

arch=$(uname -p)

BASE_IMAGE=$BASE_IMAGE_MANYLINUX2014

if [[ "$arch" == "aarch64" ]]; then
  GOSU_URL=https://github.com/tianon/gosu/releases/download/1.11/gosu-arm64
else
  GOSU_URL=https://github.com/tianon/gosu/releases/download/1.11/gosu-amd64
fi

echo "[Info] Building snowflake-sqlalchemy"
docker run \
    --rm \
    -e TERM=vt102 \
    -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
    -e LOCAL_USER_ID=$(id -u ${USER}) \
    --mount type=bind,source="${SQLALCHEMY_DIR}",target=/home/user/snowflake-sqlalchemy \
    $(docker build --pull --build-arg BASE_IMAGE=$BASE_IMAGE --build-arg GOSU_URL="$GOSU_URL" -q .) \
    /home/user/snowflake-sqlalchemy/ci/build.sh $1
