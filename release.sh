#!/usr/bin/env bash

set -eu

# Possible values: patch, minor, major
BUMP_LEVEL=$1
IMAGE_NAME="databricks-cdk-lambda"

echo "bump level: ${BUMP_LEVEL}"

## Version bump
cd typescript
NEW_VERSION=$(npm version "${BUMP_LEVEL}")
git add package.json package-lock.json
echo "new version: ${NEW_VERSION}"
rm -r dist
npx tsc
cd ../aws-lambda

poetry version "${NEW_VERSION}"
poetry update
git add pyproject.toml poetry.lock

git commit -m "Bump version to ${NEW_VERSION}"
git tag "${NEW_VERSION}"


## docker build
docker build -t "${IMAGE_NAME}:dev" .
docker tag "${IMAGE_NAME}:dev" "ffinfo/${IMAGE_NAME}:dev"
docker tag "${IMAGE_NAME}:dev" "ffinfo/${IMAGE_NAME}:${NEW_VERSION}"
docker push "ffinfo/${IMAGE_NAME}:dev"
docker push "ffinfo/${IMAGE_NAME}:${NEW_VERSION}"

# TODO: fix token
#poetry publish

cd ../typescript
npm publish

cd ../

git push
git push --tags
