#!/bin/bash

VERSION=$(git describe --abbrev=4 --dirty --always --tags | sed -E 's/-.+$//')

if [[ ! -z "$1" ]]; then
  VERSION=$1
fi

echo "Build version $VERSION"
docker buildx build --file ./build/package/Dockerfile_dev --build-arg VERSION=$VERSION --platform linux/amd64 -t gitlab-registry.ozon.ru/sre/images/file-d:$VERSION .

read -p "Do you want to push image? (y/n) " -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
echo "Cancelled by user";
exit 1;
fi

echo "Push image to registry";
docker push gitlab-registry.ozon.ru/sre/images/file-d:$VERSION