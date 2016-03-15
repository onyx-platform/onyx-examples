#!/bin/bash

set -e


if [ -z "$CIRCLE_BRANCH" ]; then
	export BR=$CI_BRANCH
else
	export BR=$CIRCLE_BRANCH
fi

i=0
files=""

FS=$(find . -name "core.clj")

for file in $FS
do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    files+=" $file"
    lein with-profile dev,circleci
  fi
  ((++i))
done

echo "Running " $files

lein with-profile dev,circle-ci test $files
