#!/bin/bash

set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

for DIR in $(ls -d */); do
	cd $DIR
	NSES=$(find src -name "*.clj" |sed s/src\\///|sed s/\\//\\./g|sed s/".clj$"//|sed s/"_"/"-"/g)

	for n in $NSES; do
        lein voom build-deps
		lein exec -ep "(require '[$n])"
	done
	cd ..
done
