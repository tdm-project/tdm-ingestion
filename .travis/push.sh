set -x
VERSION=$(cat VERSION)
echo "${CI_PASS}" | docker login -u "${CI_USER}" --password-stdin
TAG="$BACKEND-$VERSION"
docker tag "$IMG" "$IMG:$TAG"
docker push "$IMG:$TAG"
