#!/usr/bin/env bash

set -euv

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $SCRIPT_DIR

docker build -t databricks-cdk-deploy-lambda:dev .
docker tag databricks-cdk-deploy-lambda:dev ffinfo/databricks-cdk-deploy-lambda:dev
docker push ffinfo/databricks-cdk-deploy-lambda:dev
