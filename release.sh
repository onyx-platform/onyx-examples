#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

cd "$(dirname "$0")"

if [[ "$#" -ne 2 ]]; then
	echo "Usage: $0 core-version release-branch"
	echo "Example: $0 0.8.4 0.8.x"
fi

new_core_version=$1

# Update to release version.
git checkout master
git stash
git pull

for DIR in $(ls -d */); do
  cd $DIR

  lein update-dependency org.onyxplatform/onyx $new_core_version

  git add project.clj
  cd ..
done

# Aborting the commit is useful for when we're not actually
# upgrading the version, merely merging in fixes.
git commit -m "Upgrade to $new_core_version." || true
git push origin master

git checkout $2
git merge --no-edit master
git push origin $2

git checkout master
