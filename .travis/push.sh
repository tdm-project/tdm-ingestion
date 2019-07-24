VERSION=$(cat VERSION)
echo "${CI_PASS}" | docker login -u "${CI_USER}" --password-stdin
TAG="$CONF-$VERSION"
docker tag "$IMG" "$IMG:$TAG"
docker push "$IMAGE:$TAG"
