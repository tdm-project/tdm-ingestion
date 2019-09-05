#!/usr/bin/env bash
set -x
VERSION=$(cat VERSION)
echo "${CI_PASS}" | docker login -u "${CI_USER}" --password-stdin
TAG="$VERSION"
if [[ $BACKEND == "async" ]]
then
    TAG="$BACKEND-$TAG"
fi
docker tag "$IMG" "$IMG:$TAG"
docker push "$IMG:$TAG"

