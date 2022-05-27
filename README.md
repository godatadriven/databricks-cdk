[![npm version](https://badge.fury.io/js/databricks-cdk.svg)](https://badge.fury.io/js/databricks-cdk)

# databricks-cdk

This package allows the deployment of databricks workspaces and resources.

### Requirements

- cdk v2
_- AWS ssm parameters:
  - `/databricks/deploy/user`
  - `/databricks/deploy/password` (secure parameter)
  - `/databricks/account-id` (not aws account)

### Install

npm install databricks-cdk
