#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

i=0

echo '{:user {:plugins [[lein-exec "0.3.5"]]}}' > ~/.lein/profiles.clj

for DIR in $(ls -d */); do
  cd $DIR

  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
	NSES=$(find src -name "*.clj" |sed s/src\\///|sed s/\\//\\./g|sed s/".clj$"//|sed s/"_"/"-"/g)

	for n in $NSES; do
		lein exec -ep "(require '[$n])"
	done
  fi
  ((++i))

  cd ..
done
