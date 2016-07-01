#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

REPO_SRC="https://github.com/onyx-platform/onyx-release-scripts.git"
LOCAL_REPO="scripts/release-scripts"
LOCAL_REPO_VC_DIR=$LOCAL_REPO/.git

echo $PWD 1
pushd .

if [ ! -d $LOCAL_REPO_VC_DIR ]
then
echo $PWD 2
  git clone $REPO_SRC $LOCAL_REPO
else
echo $PWD 3
  cd $LOCAL_REPO
  git pull $REPO_SRC
  popd
fi
echo $PWD 4

bash "$LOCAL_REPO/release_plugin.sh" "$@"
