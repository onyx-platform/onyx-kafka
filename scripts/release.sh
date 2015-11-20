#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

cd "$(dirname "$0")/.."

if [[ "$#" -ne 2 ]]; then
	echo "Usage: $0 new-version release-branch"
	echo "Example: $0 0.8.0.4 0.8.x"
	echo "Three digit release number e.g. 0.8.0 will update onyx dependency and release plugin as 0.8.0.0"
fi

new_version=$1
release_branch=$2
current_version=`lein pprint :version | sed s/\"//g`

if [[ "$new_version" == *.*.*.* ]]; 
then 
	echo "Four digit release number "$new_version" therefore releasing plugin without updating Onyx dependency"
elif [[ "$new_version" == *.*.* ]]; 
then
	core_version=$new_version
	lein update-dependency org.onyxplatform/onyx $core_version
	new_version="$new_version.0"
else
	echo "Unhandled version number scheme. Exiting"
	exit 1
fi

# Update to release version.
git checkout master
git stash
git pull
lein set-version $new_version

sed -i.bak "s/$current_version/$new_version/g" README.md
git add README.md project.clj

git commit -m "Release version $new_version."
git tag $new_version
git push origin $new_version
git push origin master

# Merge artifacts into release branch.
git checkout $release_branch
git merge master -X theirs
git push origin $release_branch

# Prepare next release cycle.
git checkout master
lein set-version
git commit -m "Prepare for next release cycle." project.clj README.md
git push origin master
