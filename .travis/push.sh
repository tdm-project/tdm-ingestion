VERSION=$(cat VERSION)
echo "${CI_PASS}" | docker login -u "${CI_USER}" --password-stdin

docker tag $IMAGE $IMAGE:$CONF-$VERSION
docker push $IMAGE:$VERSION
