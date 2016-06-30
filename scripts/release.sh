#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

REPO_SRC="https://github.com/onyx-platform/onyx-release-scripts.git"
LOCAL_REPO="release-scripts"
LOCAL_REPO_VC_DIR=$LOCAL_REPO/.git

if [ ! -d $LOCAL_REPO_VC_DIR ]
then
  git clone $REPO_SRC $LOCAL_REPO
else
  cd $LOCAL_REPO
  git pull $REPO_SRC
  cd ..
fi

bash "$LOCAL_REPO/release_plugin.sh" "$@"
